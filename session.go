// Package windrunner is a persistent terminal session engine: it owns
// pseudo-terminals, runs an authoritative VT emulator for each one, and
// hands exact state snapshots plus live byte streams to any number of
// attached clients.
//
// The engine is deliberately unopinionated. It knows processes, terminals,
// and opaque metadata — never panes, layouts, agents, or rendering. Products
// are built on top of it, not inside it.
package windrunner

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/charmbracelet/x/vt"
	"github.com/creack/pty"
)

// DefaultScrollback bounds each session's emulator history when SpawnSpec
// does not say otherwise.
const DefaultScrollback = 2000

// SpawnSpec describes a session to start. Cols and Rows are required; the
// zero values of everything else are usable defaults.
type SpawnSpec struct {
	Command string
	Args    []string
	Dir     string
	// Env is the child's environment; nil inherits the engine process's.
	// TERM is set to xterm-256color unless the caller provides one — the
	// child must describe output for the terminal windrunner actually
	// emulates.
	Env        []string
	Cols, Rows int
	// Scrollback caps emulator history in lines; 0 means
	// DefaultScrollback.
	Scrollback int
	// Metadata travels with the session verbatim. The engine stores and
	// returns it; it never reads it.
	Metadata map[string]string
}

// Snapshot is a session's exact terminal state, serialized. Writing ANSI to
// a fresh Cols x Rows terminal reproduces the scrollback, the screen, and
// the cursor position.
type Snapshot struct {
	Cols, Rows int
	ANSI       []byte
}

// Session is one process on one PTY, with the authoritative record of what
// its terminal looks like. All methods are safe for concurrent use.
type Session struct {
	id string

	mu       sync.Mutex
	emu      *vt.Emulator
	metadata map[string]string
	title    string
	cols     int
	rows     int
	subs     map[*Subscription]struct{}
	exited   bool
	exitCode int
	closed   bool

	// writeMu serializes the two writers racing for the PTY master:
	// client input and the emulator's own query responses.
	writeMu sync.Mutex
	ptyFile *os.File

	cmd  *exec.Cmd
	done chan struct{}
}

func startSession(id string, spec SpawnSpec) (*Session, error) {
	if spec.Command == "" {
		return nil, fmt.Errorf("windrunner: spawn needs a command")
	}
	if spec.Cols < 2 || spec.Rows < 2 {
		return nil, fmt.Errorf("windrunner: %dx%d is no size for a terminal", spec.Cols, spec.Rows)
	}
	scrollback := spec.Scrollback
	if scrollback == 0 {
		scrollback = DefaultScrollback
	}

	cmd := exec.Command(spec.Command, spec.Args...)
	cmd.Dir = spec.Dir
	env := spec.Env
	if env == nil {
		env = os.Environ()
	}
	if !envHasTerm(env) {
		env = append(env, "TERM=xterm-256color")
	}
	cmd.Env = env

	ptyFile, err := pty.StartWithSize(cmd, &pty.Winsize{
		Cols: uint16(spec.Cols),
		Rows: uint16(spec.Rows),
	})
	if err != nil {
		return nil, fmt.Errorf("windrunner: start %s: %w", spec.Command, err)
	}

	emu := vt.NewEmulator(spec.Cols, spec.Rows)
	emu.Scrollback().SetMaxLines(scrollback)

	s := &Session{
		id:       id,
		emu:      emu,
		metadata: cloneMetadata(spec.Metadata),
		cols:     spec.Cols,
		rows:     spec.Rows,
		subs:     make(map[*Subscription]struct{}),
		ptyFile:  ptyFile,
		cmd:      cmd,
		done:     make(chan struct{}),
	}
	emu.SetCallbacks(vt.Callbacks{
		Title: func(title string) {
			s.mu.Lock()
			s.title = title
			s.mu.Unlock()
		},
	})

	go s.readLoop()
	go s.respondLoop()
	go s.waitLoop()
	return s, nil
}

func envHasTerm(env []string) bool {
	for _, entry := range env {
		if strings.HasPrefix(entry, "TERM=") {
			return true
		}
	}
	return false
}

func cloneMetadata(metadata map[string]string) map[string]string {
	clone := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

// readLoop is the one writer of terminal state: PTY output feeds the
// emulator and fans out to subscribers. It ends when the child exits (the
// master read fails once the slave side closes).
func (s *Session) readLoop() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.ptyFile.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			s.mu.Lock()
			s.emu.Write(chunk)
			for sub := range s.subs {
				sub.deliver(chunk, s)
			}
			s.mu.Unlock()
		}
		if err != nil {
			s.mu.Lock()
			for sub := range s.subs {
				sub.finish()
			}
			s.subs = make(map[*Subscription]struct{})
			s.mu.Unlock()
			return
		}
	}
}

// respondLoop carries the emulator's own voice back to the child: answers
// to queries (cursor position, device attributes) and in-band resize
// reports for programs that asked for them. In a tap-based design these
// bytes are garbage; owning the PTY is what makes them meaningful.
func (s *Session) respondLoop() {
	buf := make([]byte, 4096)
	for {
		n, err := s.emu.Read(buf)
		if n > 0 {
			s.writeMu.Lock()
			_, writeErr := s.ptyFile.Write(buf[:n])
			s.writeMu.Unlock()
			if writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *Session) waitLoop() {
	err := s.cmd.Wait()
	code := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		code = exitErr.ExitCode()
	} else if err != nil {
		code = -1
	}
	s.mu.Lock()
	s.exited = true
	s.exitCode = code
	s.mu.Unlock()
	close(s.done)
}

// ID names the session; stable for its whole life.
func (s *Session) ID() string { return s.id }

// Metadata returns a copy of the session's opaque tag bag.
func (s *Session) Metadata() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMetadata(s.metadata)
}

// SetMetadata replaces the session's metadata wholesale.
func (s *Session) SetMetadata(metadata map[string]string) {
	s.mu.Lock()
	s.metadata = cloneMetadata(metadata)
	s.mu.Unlock()
}

// Title reports the most recent title the program set (OSC 0/2), or "".
func (s *Session) Title() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.title
}

// Size reports the terminal's current dimensions.
func (s *Session) Size() (cols, rows int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cols, s.rows
}

// Alive reports whether the child process is still running.
func (s *Session) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.exited
}

// Done closes when the child exits. The session's terminal state remains
// attachable afterward until Close.
func (s *Session) Done() <-chan struct{} { return s.done }

// ExitCode is meaningful once Done is closed.
func (s *Session) ExitCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exitCode
}

// Write delivers input bytes to the child, exactly as typed.
func (s *Session) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.ptyFile.Write(p)
}

// Resize moves the PTY and the emulator together; the child learns via
// SIGWINCH (and in-band reports, if it asked for them).
func (s *Session) Resize(cols, rows int) error {
	if cols < 2 || rows < 2 {
		return fmt.Errorf("windrunner: %dx%d is no size for a terminal", cols, rows)
	}
	if err := pty.Setsize(s.ptyFile, &pty.Winsize{
		Cols: uint16(cols),
		Rows: uint16(rows),
	}); err != nil {
		return fmt.Errorf("windrunner: resize pty: %w", err)
	}
	s.mu.Lock()
	s.emu.Resize(cols, rows)
	s.cols, s.rows = cols, rows
	s.mu.Unlock()
	return nil
}

// Snapshot serializes the terminal's exact current state.
func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Session) snapshotLocked() Snapshot {
	var out strings.Builder
	back := s.emu.Scrollback()
	for index := 0; index < back.Len(); index++ {
		out.WriteString(back.Line(index).Render())
		out.WriteString("\r\n")
	}
	// Render joins rows with bare newlines; a replaying terminal needs the
	// carriage returns too, or every row after a non-empty one drifts
	// rightward by the previous row's width.
	out.WriteString(strings.ReplaceAll(s.emu.Render(), "\n", "\r\n"))
	cursor := s.emu.CursorPosition()
	fmt.Fprintf(&out, "\x1b[0m\x1b[%d;%dH", cursor.Y+1, cursor.X+1)
	return Snapshot{Cols: s.cols, Rows: s.rows, ANSI: []byte(out.String())}
}

// Attach returns the session's state right now and a subscription that
// carries every output byte after it — atomically, so nothing falls in the
// gap between snapshot and stream.
func (s *Session) Attach(buffer int) (Snapshot, *Subscription) {
	if buffer <= 0 {
		buffer = 64
	}
	sub := &Subscription{ch: make(chan []byte, buffer)}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.snapshotLocked()
	if s.exited || s.closed {
		// The stream is already over; hand back a finished subscription
		// rather than one that will never speak.
		sub.finish()
		return snapshot, sub
	}
	s.subs[sub] = struct{}{}
	return snapshot, sub
}

// Kill forcibly ends the child process. Terminal state survives until
// Close.
func (s *Session) Kill() error {
	s.mu.Lock()
	exited := s.exited
	s.mu.Unlock()
	if exited {
		return nil
	}
	if s.cmd.Process == nil {
		return nil
	}
	return s.cmd.Process.Signal(syscall.SIGKILL)
}

// close releases everything: the child, the PTY, the emulator, and every
// subscriber. Idempotent. Reached through Engine.Close or Engine.Remove.
func (s *Session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	subs := s.subs
	s.subs = make(map[*Subscription]struct{})
	s.mu.Unlock()

	for sub := range subs {
		sub.finish()
	}
	s.Kill()
	// Closing the master hangs up the slave side; the read loop ends on
	// the resulting error.
	s.ptyFile.Close()
	// Close the emulator's response pipe through the pipe writer — the
	// pipe's own methods are synchronized — so respondLoop sees EOF.
	if pw, ok := s.emu.InputPipe().(interface{ Close() error }); ok {
		pw.Close()
	}
}

// Subscription is one attached reader of a session's output stream.
type Subscription struct {
	ch     chan []byte
	mu     sync.Mutex
	done   bool
	lagged bool
}

// Output yields chunks of raw terminal output. The channel closes when the
// session ends, the subscription is closed, or the reader falls too far
// behind (see Lagged).
func (sub *Subscription) Output() <-chan []byte { return sub.ch }

// Lagged reports whether the subscription was dropped for reading too
// slowly. The recovery is to attach again: a fresh snapshot is always
// cheaper than an unbounded backlog.
func (sub *Subscription) Lagged() bool {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.lagged
}

// Close detaches the subscription. Safe to call at any time, any number of
// times.
func (sub *Subscription) Close() {
	sub.mu.Lock()
	if !sub.done {
		sub.done = true
		close(sub.ch)
	}
	sub.mu.Unlock()
}

// deliver hands a chunk to the subscriber without ever blocking the read
// loop: a full buffer marks the subscriber lagged and drops it.
func (sub *Subscription) deliver(chunk []byte, s *Session) {
	sub.mu.Lock()
	if sub.done {
		sub.mu.Unlock()
		return
	}
	select {
	case sub.ch <- chunk:
		sub.mu.Unlock()
	default:
		sub.lagged = true
		sub.done = true
		close(sub.ch)
		sub.mu.Unlock()
		delete(s.subs, sub)
	}
}

// finish ends the stream normally.
func (sub *Subscription) finish() {
	sub.mu.Lock()
	if !sub.done {
		sub.done = true
		close(sub.ch)
	}
	sub.mu.Unlock()
}
