package windrunner

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/vt"
)

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine := NewEngine()
	t.Cleanup(engine.Close)
	return engine
}

func spawnShell(t *testing.T, engine *Engine, script string) *Session {
	t.Helper()
	s, err := engine.Spawn(SpawnSpec{
		Command: "/bin/sh",
		Args:    []string{"-c", script},
		Cols:    80,
		Rows:    24,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	return s
}

func sessionText(s *Session) string {
	return stripANSI(string(s.Snapshot().ANSI))
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[a-zA-Z]|\x1b\][^\x07]*\x07`)

func stripANSI(text string) string {
	return ansiPattern.ReplaceAllString(text, "")
}

func TestOutputReachesEmulatorAndSubscribers(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `printf 'hello from the pty\n'; sleep 60`)

	_, sub := s.Attach(64)
	defer sub.Close()

	waitFor(t, "output in snapshot", func() bool {
		return strings.Contains(sessionText(s), "hello from the pty")
	})

	var streamed bytes.Buffer
	deadline := time.After(5 * time.Second)
	for !strings.Contains(streamed.String(), "hello from the pty") {
		select {
		case chunk, ok := <-sub.Output():
			if !ok {
				t.Fatalf("stream closed early; got %q", streamed.String())
			}
			streamed.Write(chunk)
		case <-deadline:
			t.Fatalf("streamed output missing greeting: %q", streamed.String())
		}
	}
}

func TestInputReachesChild(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `read line; printf 'echoed:%s\n' "$line"; sleep 60`)

	if _, err := s.Write([]byte("windrunner\r")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitFor(t, "echoed input", func() bool {
		return strings.Contains(sessionText(s), "echoed:windrunner")
	})
}

// TestChildQueriesGetRealAnswers is the reason this engine owns the PTY:
// a program that asks its terminal where the cursor is must get an answer
// from the authoritative emulator, not silence.
func TestChildQueriesGetRealAnswers(t *testing.T) {
	engine := newTestEngine(t)
	// Ask for a cursor position report (DSR 6) and read the reply back.
	// Raw mode matters: canonical-mode stdin would buffer the ESC[row;colR
	// reply forever waiting for a newline that never comes.
	s := spawnShell(t, engine,
		`stty raw -echo; printf '\033[6n'; reply=$(dd bs=1 count=6 2>/dev/null); stty sane; printf 'reply:%s:end\n' "$reply" | od -An -c; sleep 60`)

	waitFor(t, "cursor position reply", func() bool {
		return strings.Contains(sessionText(s), "033   [")
	})
	// od renders the reply bytes; the emulator's answer must include the
	// CSI introducer and a digits;digits shape.
	text := sessionText(s)
	if !regexp.MustCompile(`033\s+\[[\s0-9;]+`).MatchString(text) {
		t.Fatalf("child got no usable DSR answer:\n%s", text)
	}
}

func TestSnapshotCarriesScrollback(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `i=1; while [ $i -le 40 ]; do printf 'line %02d\n' $i; i=$((i+1)); done; sleep 60`)

	waitFor(t, "final line", func() bool {
		return strings.Contains(sessionText(s), "line 40")
	})
	// 40 lines through a 24-row terminal: the early lines live only in
	// scrollback, and the snapshot must still carry them.
	if !strings.Contains(sessionText(s), "line 01") {
		t.Fatalf("snapshot lost scrolled-off history:\n%s", sessionText(s))
	}
}

func TestAttachAfterExitYieldsStateAndClosedStream(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `printf 'parting words\n'; exit 3`)

	<-s.Done()
	if code := s.ExitCode(); code != 3 {
		t.Fatalf("exit code %d, want 3", code)
	}
	waitFor(t, "output drained into emulator", func() bool {
		return strings.Contains(sessionText(s), "parting words")
	})

	snapshot, sub := s.Attach(8)
	if !strings.Contains(stripANSI(string(snapshot.ANSI)), "parting words") {
		t.Fatal("post-exit snapshot lost the terminal state")
	}
	if _, open := <-sub.Output(); open {
		t.Fatal("post-exit subscription should be closed")
	}
}

func TestResizePropagatesToChildAndEmulator(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `trap 'printf "resized to $(stty size)\n"' WINCH; printf ready\n; while :; do sleep 0.1; done`)

	waitFor(t, "child ready", func() bool {
		return strings.Contains(sessionText(s), "ready")
	})
	if err := s.Resize(100, 30); err != nil {
		t.Fatalf("Resize: %v", err)
	}
	if cols, rows := s.Size(); cols != 100 || rows != 30 {
		t.Fatalf("engine size %dx%d, want 100x30", cols, rows)
	}
	waitFor(t, "SIGWINCH landed", func() bool {
		return strings.Contains(sessionText(s), "resized to 30 100")
	})
}

func TestLaggedSubscriberIsDroppedNotBlocking(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `i=0; while [ $i -le 500 ]; do printf 'flood %d\n' $i; i=$((i+1)); done; sleep 60`)

	_, sub := s.Attach(1) // tiny buffer, never read
	waitFor(t, "lagged drop", func() bool { return sub.Lagged() })
	if _, open := <-sub.Output(); open {
		// Drain one pending chunk is fine; the channel must close soon.
		waitFor(t, "channel close", func() bool {
			_, stillOpen := <-sub.Output()
			return !stillOpen
		})
	}
	// The session itself must be unharmed by the slow reader.
	waitFor(t, "flood completion", func() bool {
		return strings.Contains(sessionText(s), "flood 500")
	})
}

func TestMetadataRoundTrips(t *testing.T) {
	engine := newTestEngine(t)
	s, err := engine.Spawn(SpawnSpec{
		Command:  "/bin/sh",
		Args:     []string{"-c", "sleep 60"},
		Cols:     80,
		Rows:     24,
		Metadata: map[string]string{"task": "prove metadata", "owner": "trent"},
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if got := s.Metadata()["task"]; got != "prove metadata" {
		t.Fatalf("metadata lost: %q", got)
	}
	s.SetMetadata(map[string]string{"task": "replaced"})
	if got := s.Metadata(); got["task"] != "replaced" || got["owner"] != "" {
		t.Fatalf("SetMetadata is not wholesale: %v", got)
	}
}

func TestRemoveEndsEverything(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `sleep 60`)
	_, sub := s.Attach(8)

	engine.Remove(s.ID())
	if _, ok := engine.Session(s.ID()); ok {
		t.Fatal("removed session still listed")
	}
	waitFor(t, "child death", func() bool { return !s.Alive() })
	waitFor(t, "subscriber stream end", func() bool {
		_, open := <-sub.Output()
		return !open
	})
}

func TestSnapshotReplaysIntoFreshEmulator(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `printf '\033[1;31mred alert\033[0m plain\n'; sleep 60`)
	waitFor(t, "styled output", func() bool {
		return strings.Contains(sessionText(s), "red alert")
	})

	// The contract: writing Snapshot.ANSI to a fresh emulator of the same
	// size reproduces the state exactly. This is what a client renderer
	// does on attach, so the test does it literally.
	snapshot := s.Snapshot()
	if !bytes.Contains(snapshot.ANSI, []byte("[31")) {
		t.Fatal("snapshot dropped styling")
	}
	replayed := vt.NewEmulator(snapshot.Cols, snapshot.Rows)
	replayed.Write(snapshot.ANSI)
	source, echo := s.Snapshot(), replayed.Render()
	sourceEmu := vt.NewEmulator(source.Cols, source.Rows)
	sourceEmu.Write(source.ANSI)
	if got, want := echo, sourceEmu.Render(); got != want {
		t.Fatalf("replay drifted from source:\n got: %q\nwant: %q", got, want)
	}
	if !strings.Contains(stripANSI(echo), "red alert") {
		t.Fatalf("replayed screen lost content:\n%s", stripANSI(echo))
	}
}

// TestQueryFloodWithDeafChildDoesNotWedgeTheEngine reproduces the daemon
// freeze found with a real agent TUI: a child that emits terminal queries
// while never reading its stdin. Its input buffer fills, the response
// writes block — and before the elastic response queue, that blocked the
// emulator write under the session lock and froze every Snapshot behind
// one rude program.
func TestQueryFloodWithDeafChildDoesNotWedgeTheEngine(t *testing.T) {
	engine := newTestEngine(t)
	// 4000 cursor-position queries, stdin never read: each answer is ~8
	// bytes against a kernel input queue of a few KB, so the PTY input
	// side is guaranteed to jam.
	// Raw mode is the load-bearing detail: canonical mode discards input
	// when the queue fills, raw mode blocks the writer — and agent TUIs
	// run raw.
	s := spawnShell(t, engine,
		`stty raw -echo; i=0; while [ $i -lt 4000 ]; do printf '\033[6n'; i=$((i+1)); done; printf 'flood done\r\n'; sleep 60`)

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(15 * time.Second)
		for !strings.Contains(sessionText(s), "flood done") {
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("engine wedged: Snapshot never returned")
	}
	if !strings.Contains(sessionText(s), "flood done") {
		t.Fatalf("child never finished its flood:\n%s", sessionText(s))
	}
}
