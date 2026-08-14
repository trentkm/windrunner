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
			streamed.Write(chunk.Bytes)
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

func nextEvent(t *testing.T, sub *EventSubscription) Event {
	t.Helper()
	select {
	case event, ok := <-sub.Events():
		if !ok {
			t.Fatal("event stream closed early")
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")
	}
	panic("unreachable")
}

// TestEventsTellTheSessionStory walks the pub/sub stream through a
// session's whole life: spawned, quiet (idle), spoken to (busy), and
// removed — the coordination signals richer than poke-and-peek.
func TestEventsTellTheSessionStory(t *testing.T) {
	engine := newTestEngine(t)
	sub := engine.Subscribe(64)
	defer sub.Close()

	s, err := engine.Spawn(SpawnSpec{
		Command:   "/bin/sh",
		Args:      []string{"-c", `printf 'hi\n'; read line; printf 'again\n'; sleep 60`},
		Cols:      80,
		Rows:      24,
		IdleAfter: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	if event := nextEvent(t, sub); event.Type != EventSpawned || event.SessionID != s.ID() || event.Command != "/bin/sh" {
		t.Fatalf("first event should announce the spawn: %+v", event)
	}
	// Sessions start busy, so the greeting produces no event; the first
	// activity signal is the session going quiet.
	if event := nextEvent(t, sub); event.Type != EventIdle || event.SessionID != s.ID() {
		t.Fatalf("expected idle after the greeting went quiet: %+v", event)
	}
	// Input wakes the child; its output is the busy transition.
	if _, err := s.Write([]byte("wake\r")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if event := nextEvent(t, sub); event.Type != EventBusy || event.SessionID != s.ID() {
		t.Fatalf("expected busy once output resumed: %+v", event)
	}

	// Remove kills the child; exited and removed both arrive, in
	// whichever order the races land (idle may slip in beforehand).
	engine.Remove(s.ID())
	seen := map[EventType]bool{}
	for !seen[EventExited] || !seen[EventRemoved] {
		event := nextEvent(t, sub)
		if event.SessionID != s.ID() {
			t.Fatalf("event for a stranger: %+v", event)
		}
		if event.Type == EventExited && event.ExitCode == 0 {
			t.Fatalf("SIGKILLed child reported exit code 0")
		}
		seen[event.Type] = true
	}
}

// TestChildKnowsItsOwnSession: the engine stamps WINDRUNNER_SESSION into
// each child's environment, so a process inside a session can name itself
// to the control plane — the attribution behind audited peer sends.
func TestChildKnowsItsOwnSession(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `printf 'sid:%s:end\n' "$WINDRUNNER_SESSION"; sleep 60`)
	waitFor(t, "session id in child env", func() bool {
		return strings.Contains(sessionText(s), "sid:"+s.ID()+":end")
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

// TestTitleSettingDoesNotDeadlock: OSC titles arrive through a callback
// that fires inside emu.Write — under the session lock. The callback must
// therefore never take the lock itself; the first real TUI (they all set
// titles) proved this the hard way.
func TestTitleSettingDoesNotDeadlock(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `printf '\033]2;herd of one\007'; printf 'titled\n'; sleep 60`)
	waitFor(t, "output after title", func() bool {
		return strings.Contains(sessionText(s), "titled")
	})
	if got := s.Title(); got != "herd of one" {
		t.Fatalf("title = %q, want %q", got, "herd of one")
	}
}

// A subscriber's replica paints in-flight bytes at whatever size it holds,
// so a resize must push the re-wrapped truth through the stream itself —
// ordered against output by the session lock — or replicas diverge until
// the child happens to repaint every cell.
func TestResizeBroadcastsARepaintToSubscribers(t *testing.T) {
	engine := newTestEngine(t)
	s := spawnShell(t, engine, `printf 'landmark\n'; while :; do sleep 0.1; done`)

	waitFor(t, "landmark drawn", func() bool {
		return strings.Contains(sessionText(s), "landmark")
	})
	_, sub := s.Attach(16)
	if err := s.Resize(100, 30); err != nil {
		t.Fatalf("Resize: %v", err)
	}

	deadline := time.After(2 * time.Second)
	var received strings.Builder
	for {
		select {
		case chunk, open := <-sub.Output():
			if !open {
				t.Fatal("subscription closed before the repaint arrived")
			}
			if chunk.Resize != nil &&
				(chunk.Resize.Cols != 100 || chunk.Resize.Rows != 30) {
				t.Fatalf("resize notice = %+v", chunk.Resize)
			}
			received.Write(chunk.Bytes)
		case <-deadline:
			t.Fatalf("no repaint after resize; received %q", received.String())
		}
		text := received.String()
		if !strings.Contains(text, "\x1b[H\x1b[2J") {
			continue
		}
		// The repaint carries the re-wrapped screen and parks the cursor.
		after := text[strings.Index(text, "\x1b[H\x1b[2J"):]
		if !strings.Contains(stripANSI(after), "landmark") {
			continue
		}
		if !regexp.MustCompile(`\x1b\[\d+;\d+H`).MatchString(after) {
			t.Fatalf("repaint did not restore the cursor: %q", after)
		}
		return
	}
}

// An attach snapshot taken while the PTY has delivered only half of an
// escape sequence must not strand the tail: a fresh replica that missed
// the head sees raw text and prints it at the cursor — a terminal title
// like "Claude Code" materializing on an app's input line.
func TestAttachMidEscapeSequenceDoesNotLeakTheTail(t *testing.T) {
	engine := newTestEngine(t)
	// The child emits half an OSC title, stalls, then the rest plus a
	// landmark — two PTY reads with the seam inside the sequence.
	s := spawnShell(t, engine,
		`printf 'ground\033]0;Claude'; sleep 0.4; printf ' Code\007landmark\n'; while :; do sleep 0.1; done`)

	waitFor(t, "first half arrived", func() bool {
		return strings.Contains(sessionText(s), "ground")
	})
	snapshot, sub := s.Attach(64)
	defer sub.Close()

	replica := vt.NewEmulator(snapshot.Cols, snapshot.Rows)
	replica.Write(snapshot.ANSI)
	deadline := time.After(3 * time.Second)
	for {
		screen := stripANSI(replica.Render())
		if strings.Contains(screen, "landmark") {
			if strings.Contains(screen, "Code") {
				t.Fatalf("title tail leaked into the replica's cells:\n%s", screen)
			}
			return
		}
		select {
		case message, open := <-sub.Output():
			if !open {
				t.Fatal("stream ended before the landmark")
			}
			replica.Write(message.Bytes)
		case <-deadline:
			t.Fatalf("landmark never arrived; screen:\n%s", screen)
		}
	}
}

func TestCompleteBoundaryHoldsBackTornTails(t *testing.T) {
	for _, test := range []struct {
		name  string
		chunk string
		cut   int
	}{
		{"plain text passes whole", "hello", 5},
		{"complete CSI passes whole", "a\x1b[31m", 6},
		{"torn CSI held from ESC", "ab\x1b[3", 2},
		{"bare trailing ESC held", "abc\x1b", 3},
		{"complete OSC with BEL", "\x1b]0;t\x07x", 7},
		{"torn OSC title held", "ok\x1b]0;Claude", 2},
		{"OSC waiting on ST's backslash", "ok\x1b]0;t\x1b", 2},
		{"complete OSC with ST", "\x1b]0;t\x1b\\z", 8},
		{"charset designation is whole", "\x1b(Bx", 4},
		{"torn charset designation held", "x\x1b(", 1},
		{"torn UTF-8 rune held", "ab\xe2\x9c", 2},
		{"complete UTF-8 passes", "ab\xe2\x9c\xb3", 5},
	} {
		if got := completeBoundary([]byte(test.chunk)); got != test.cut {
			t.Errorf("%s: completeBoundary(%q) = %d, want %d",
				test.name, test.chunk, got, test.cut)
		}
	}
}

// A program that asks the terminal to report its title (CSI 21 t) must
// not get the title typed into its stdin: real terminals refuse this
// query precisely because the response is indistinguishable from typing.
func TestTitleReportIsNotAnsweredIntoStdin(t *testing.T) {
	engine := newTestEngine(t)
	// Set a title, ask for it back, then echo stdin to stdout forever:
	// anything the emulator answers becomes visible screen text.
	s := spawnShell(t, engine,
		`printf '\033]0;SECRET-TITLE\007\033[21t'; cat`)

	time.Sleep(600 * time.Millisecond)
	if text := sessionText(s); strings.Contains(text, "SECRET-TITLE") {
		t.Fatalf("title report reached the child's stdin:\n%q", text)
	}
}

// The snapshot carries cells but the terminal holds more state than
// cells: a program that set scroll margins scrolls inside them, and a
// replica seeded without the margins scrolls the whole screen — rows
// drift apart permanently. Claude Code manages margins around its input
// box, which is how header text ended up parked on the input line.
func TestAttachSeedCarriesScrollMargins(t *testing.T) {
	engine := newTestEngine(t)
	// Pin rows 1-3 as a scroll region, park a footer on row 10, then
	// scroll inside the region after the replica attaches.
	s := spawnShell(t, engine,
		`printf '\033[10;1HFOOTER-STAYS'; printf '\033[1;3r\033[1;1Hone\r\ntwo\r\nthree'; printf ready; sleep 0.4; printf '\r\nfour\r\nfive'; sleep 60`)

	waitFor(t, "region painted", func() bool {
		return strings.Contains(sessionText(s), "ready")
	})
	snapshot, sub := s.Attach(64)
	defer sub.Close()
	replica := vt.NewEmulator(snapshot.Cols, snapshot.Rows)
	replica.Write(snapshot.ANSI)

	deadline := time.After(3 * time.Second)
	for {
		local := stripANSI(replica.Render())
		if strings.Contains(local, "five") {
			truth := stripANSI(s.Snapshot().renderInto(t))
			if local != truth {
				t.Fatalf("replica diverged from the session:\nreplica:\n%s\n\nsession:\n%s",
					local, truth)
			}
			return
		}
		select {
		case message, open := <-sub.Output():
			if !open {
				t.Fatal("stream ended early")
			}
			replica.Write(message.Bytes)
		case <-deadline:
			t.Fatalf("scrolled rows never arrived:\n%s", local)
		}
	}
}

// renderInto replays a snapshot into a fresh emulator and renders it —
// what any attaching client would see.
func (snapshot Snapshot) renderInto(t *testing.T) string {
	t.Helper()
	emu := vt.NewEmulator(snapshot.Cols, snapshot.Rows)
	emu.Write(snapshot.ANSI)
	return emu.Render()
}
