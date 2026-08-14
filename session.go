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
	"time"

	"github.com/charmbracelet/x/vt"
	"github.com/creack/pty"
)

// DefaultScrollback bounds each session's emulator history when SpawnSpec
// does not say otherwise.
const DefaultScrollback = 2000

// DefaultIdleAfter is how long a session's terminal must stay quiet before
// it is reported idle, when SpawnSpec does not say otherwise.
const DefaultIdleAfter = 2 * time.Second

// SpawnSpec describes a session to start. Cols and Rows are required; the
// zero values of everything else are usable defaults.
type SpawnSpec struct {
	Command string
	Args    []string
	Dir     string
	// Env is the child's environment; nil inherits the engine process's.
	// TERM is set to xterm-256color unless the caller provides one — the
	// child must describe output for the terminal windrunner actually
	// emulates. WINDRUNNER_SESSION is set to the session's ID unless the
	// caller provides one, so a session's process can name itself to the
	// control plane.
	Env        []string
	Cols, Rows int
	// Scrollback caps emulator history in lines; 0 means
	// DefaultScrollback.
	Scrollback int
	// IdleAfter is the quiet window behind idle/busy events: no output for
	// this long means idle. 0 means DefaultIdleAfter.
	IdleAfter time.Duration
	// Peer opts the session into peer input: writes to its stdin that
	// arrive without an attachment, the way automation and other sessions
	// speak. The engine only stores the bit — Write never checks it; the
	// server package refuses the detached input op for sessions that did
	// not opt in.
	Peer bool
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
	id   string
	peer bool

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

	// Idle/busy events, published to the engine's subscribers. busy flips
	// on output and idleTimer flips it back after idleAfter of quiet;
	// publish is engine-provided and must be called without holding mu.
	publish   func(Event)
	busy      bool
	idleAfter time.Duration
	idleTimer *time.Timer
}

func startSession(id string, spec SpawnSpec, publish func(Event)) (*Session, error) {
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
	idleAfter := spec.IdleAfter
	if idleAfter == 0 {
		idleAfter = DefaultIdleAfter
	}

	cmd := exec.Command(spec.Command, spec.Args...)
	cmd.Dir = spec.Dir
	env := spec.Env
	if env == nil {
		env = os.Environ()
	}
	if !envHas(env, "TERM") {
		env = append(env, "TERM=xterm-256color")
	}
	if !envHas(env, "WINDRUNNER_SESSION") {
		env = append(env, "WINDRUNNER_SESSION="+id)
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
		peer:     spec.Peer,
		emu:      emu,
		metadata: cloneMetadata(spec.Metadata),
		cols:     spec.Cols,
		rows:     spec.Rows,
		subs:     make(map[*Subscription]struct{}),
		ptyFile:  ptyFile,
		cmd:      cmd,
		done:     make(chan struct{}),
		// Born busy: the process is presumably about to speak, and
		// starting busy means the first event a subscriber sees is the
		// meaningful one — the session going quiet.
		publish:   publish,
		busy:      true,
		idleAfter: idleAfter,
	}
	s.idleTimer = time.AfterFunc(idleAfter, s.goIdle)
	emu.SetCallbacks(vt.Callbacks{
		Title: func(title string) {
			// No lock: callbacks fire synchronously from inside emulator
			// writes, and every emulator write in this file already holds
			// mu — taking it here is self-deadlock, sprung by the first
			// program that sets a title (they all do).
			s.title = title
		},
	})

	go s.readLoop()
	go s.respondLoop()
	go s.waitLoop()
	return s, nil
}

func envHas(env []string, name string) bool {
	prefix := name + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
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
			// Output means busy; announce the transition only for a live
			// child (post-exit PTY drain is an epilogue, not activity).
			announce := !s.busy && !s.exited
			s.busy = true
			s.emu.Write(chunk)
			for sub := range s.subs {
				sub.deliver(Message{Bytes: chunk}, s)
			}
			s.mu.Unlock()
			s.idleTimer.Reset(s.idleAfter)
			if announce {
				s.publish(Event{Type: EventBusy, SessionID: s.id})
			}
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
//
// The two halves are decoupled through an elastic queue, and that is not
// an optimization: the emulator's response pipe is unbuffered and its
// writes happen inside emu.Write — under the session lock. A child that
// emits queries while not draining its own stdin (a busy TUI mid-burst)
// blocks the PTY write; if that write were taken synchronously off the
// pipe, the next response would block emu.Write, wedge the lock, and
// freeze every List and Snapshot in the daemon behind one rude program.
// Found with a real agent TUI, the hard way.
func (s *Session) respondLoop() {
	responses := make(chan []byte, 256)
	go func() {
		defer close(responses)
		buf := make([]byte, 4096)
		for {
			n, err := s.emu.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				select {
				case responses <- chunk:
				default:
					// The child has ignored its input long enough to
					// back up 256 answers; late answers are worse than
					// none.
				}
			}
			if err != nil {
				return
			}
		}
	}()
	for chunk := range responses {
		s.writeMu.Lock()
		_, err := s.ptyFile.Write(chunk)
		s.writeMu.Unlock()
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
	// Exited is the terminal signal; a trailing idle would only echo it.
	s.idleTimer.Stop()
	close(s.done)
}

// goIdle fires when the terminal has been quiet for the idle window.
func (s *Session) goIdle() {
	s.mu.Lock()
	if s.closed || s.exited || !s.busy {
		s.mu.Unlock()
		return
	}
	s.busy = false
	s.mu.Unlock()
	s.publish(Event{Type: EventIdle, SessionID: s.id})
}

// ID names the session; stable for its whole life.
func (s *Session) ID() string { return s.id }

// Peer reports whether the session opted into peer input at spawn.
// Immutable for the session's life — a guardrail that could be flipped
// after the fact would not be one.
func (s *Session) Peer() bool { return s.peer }

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
	// A resize re-wraps this emulator, but a subscriber's replica has been
	// painting the in-flight bytes at whatever size it reached first — the
	// two grids diverge in the window between the client resizing its
	// replica and this resize landing, and the child's SIGWINCH repaint is
	// allowed to be partial, so the divergence can be permanent. Broadcast
	// the re-wrapped screen into the stream itself: delivery holds the same
	// lock as the output pump, so every subscriber sees old-size bytes,
	// then this repaint, then new-size bytes, in exactly that order, and
	// converges by construction.
	message := Message{
		Bytes:  s.screenRepaintLocked(),
		Resize: &Resize{Cols: cols, Rows: rows},
	}
	for sub := range s.subs {
		sub.deliver(message, s)
	}
	s.mu.Unlock()
	return nil
}

// screenRepaintLocked serializes the live screen as a clear-and-repaint:
// home, clear, every row, cursor restored. Screen only — scrollback is
// already in every replica's history, and replaying it would double it.
func (s *Session) screenRepaintLocked() []byte {
	var out strings.Builder
	out.WriteString("\x1b[0m\x1b[H\x1b[2J")
	out.WriteString(strings.ReplaceAll(s.emu.Render(), "\n", "\r\n"))
	cursor := s.emu.CursorPosition()
	fmt.Fprintf(&out, "\x1b[0m\x1b[%d;%dH", cursor.Y+1, cursor.X+1)
	return []byte(out.String())
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
	sub := &Subscription{ch: make(chan Message, buffer)}
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

	s.idleTimer.Stop()
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
// Message is one delivery on a subscription: terminal output bytes, and —
// when the session's terminal moved under the subscriber — the new size,
// carried with the repaint those bytes hold so a replica resizes and
// repaints as one ordered step.
type Message struct {
	Bytes  []byte
	Resize *Resize
}

// Resize is a terminal size change riding the stream.
type Resize struct {
	Cols, Rows int
}

type Subscription struct {
	ch     chan Message
	mu     sync.Mutex
	done   bool
	lagged bool
}

// Output yields the stream's messages in terminal order. The channel
// closes when the session ends, the subscription is closed, or the reader
// falls too far behind (see Lagged).
func (sub *Subscription) Output() <-chan Message { return sub.ch }

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
func (sub *Subscription) deliver(message Message, s *Session) {
	sub.mu.Lock()
	if sub.done {
		sub.mu.Unlock()
		return
	}
	select {
	case sub.ch <- message:
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
