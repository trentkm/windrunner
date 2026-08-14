package server

import (
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/trentkm/windrunner"
	"github.com/trentkm/windrunner/client"
	"github.com/trentkm/windrunner/wire"
)

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[a-zA-Z]|\x1b\][^\x07]*\x07`)

func stripANSI(text string) string {
	return ansiPattern.ReplaceAllString(text, "")
}

func startStack(t *testing.T, options ...Option) *client.Client {
	t.Helper()
	// Not t.TempDir(): unix socket paths cap at 104 bytes on macOS, and
	// long test names push the per-test dir past it.
	dir, err := os.MkdirTemp("", "wr")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "wr.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	engine := windrunner.NewEngine()
	t.Cleanup(engine.Close)
	go Serve(engine, listener, options...)
	t.Cleanup(func() { listener.Close() })

	c, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func drainUntil(t *testing.T, a *client.Attachment, want string) string {
	t.Helper()
	var collected strings.Builder
	deadline := time.After(5 * time.Second)
	for !strings.Contains(stripANSI(collected.String()), want) {
		select {
		case chunk, ok := <-a.Output():
			if !ok {
				t.Fatalf("stream closed before %q arrived; got %q", want, collected.String())
			}
			collected.Write(chunk)
		case <-deadline:
			t.Fatalf("timed out waiting for %q; got %q", want, stripANSI(collected.String()))
		}
	}
	return collected.String()
}

func TestFullStackRoundTrip(t *testing.T) {
	c := startStack(t)

	info, err := c.Spawn(wire.Request{
		Command:  "/bin/sh",
		Args:     []string{"-c", `printf 'stack alive\n'; read line; printf 'got:%s\n' "$line"; sleep 60`},
		Cols:     80,
		Rows:     24,
		Metadata: map[string]string{"name": "roundtrip"},
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// First attachment sees the greeting (snapshot or stream, depending
	// on timing), sends input, sees the reply streamed.
	a, err := c.Attach(info.ID, 64)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	combined := stripANSI(string(a.Snapshot().ANSI))
	if !strings.Contains(combined, "stack alive") {
		combined += stripANSI(drainUntil(t, a, "stack alive"))
	}
	if err := a.Write([]byte("over the wire\r")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	drainUntil(t, a, "got:over the wire")
	a.Close()

	// A second attachment's snapshot must carry the whole story: the
	// session state outlived the first client.
	b, err := c.Attach(info.ID, 64)
	if err != nil {
		t.Fatalf("re-Attach: %v", err)
	}
	defer b.Close()
	replayed := stripANSI(string(b.Snapshot().ANSI))
	for _, want := range []string{"stack alive", "got:over the wire"} {
		if !strings.Contains(replayed, want) {
			t.Fatalf("re-attach snapshot missing %q:\n%s", want, replayed)
		}
	}

	// Listing reflects the metadata; kill + remove end the story.
	sessions, err := c.List()
	if err != nil || len(sessions) != 1 {
		t.Fatalf("List: %v, %d sessions", err, len(sessions))
	}
	if sessions[0].Metadata["name"] != "roundtrip" {
		t.Fatalf("metadata lost over the wire: %v", sessions[0].Metadata)
	}
	if err := c.Remove(info.ID); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if sessions, _ := c.List(); len(sessions) != 0 {
		t.Fatalf("session survived Remove: %v", sessions)
	}
}

func TestExitReachesAttachedClient(t *testing.T) {
	c := startStack(t)
	info, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `sleep 0.2; exit 7`},
		Cols:    80,
		Rows:    24,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	a, err := c.Attach(info.ID, 64)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer a.Close()
	select {
	case code := <-a.Exited():
		if code != 7 {
			t.Fatalf("exit code %d, want 7", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exit never reached the client")
	}
}

func TestResizeOverTheWire(t *testing.T) {
	c := startStack(t)
	info, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `while :; do sleep 0.1; done`},
		Cols:    80,
		Rows:    24,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := c.Resize(info.ID, 132, 43); err != nil {
		t.Fatalf("Resize: %v", err)
	}
	resized, err := c.Info(info.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if resized.Cols != 132 || resized.Rows != 43 {
		t.Fatalf("size %dx%d, want 132x43", resized.Cols, resized.Rows)
	}
}

func TestInputAndSnapshotWithoutAttachment(t *testing.T) {
	c := startStack(t)
	info, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `read line; printf 'heard:%s\n' "$line"; sleep 60`},
		Cols:    80,
		Rows:    24,
		Peer:    true,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := c.Input(info.ID, []byte("no attachment needed\r")); err != nil {
		t.Fatalf("Input: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := c.Snapshot(info.ID)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if strings.Contains(stripANSI(string(snapshot.ANSI)), "heard:no attachment needed") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("input never landed:\n%s", stripANSI(string(snapshot.ANSI)))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPeerInputIsOptIn: sending into a session is remote code execution
// as far as its process is concerned, so the detached input op must be
// refused unless the session was spawned with peer access. Attachments
// are unaffected either way.
func TestPeerInputIsOptIn(t *testing.T) {
	c := startStack(t)
	info, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `read line; printf 'heard:%s\n' "$line"; sleep 60`},
		Cols:    80,
		Rows:    24,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if info.Peer {
		t.Fatal("peer access must default off")
	}
	if err := c.Input(info.ID, []byte("intruder\r")); err == nil {
		t.Fatal("detached input into a non-peer session must be refused")
	} else if !strings.Contains(err.Error(), "peer") {
		t.Fatalf("refusal should say why: %v", err)
	}

	// The gate is the op, not the session: an attachment still speaks.
	a, err := c.Attach(info.ID, 64)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer a.Close()
	if err := a.Write([]byte("attached\r")); err != nil {
		t.Fatalf("attached input should not be gated: %v", err)
	}
	drainUntil(t, a, "heard:attached")
}

func TestEventsOverTheWire(t *testing.T) {
	c := startStack(t)
	stream, err := c.Subscribe(64)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer stream.Close()

	info, err := c.Spawn(wire.Request{
		Command:     "/bin/sh",
		Args:        []string{"-c", `printf 'hi\n'; sleep 60`},
		Cols:        80,
		Rows:        24,
		IdleAfterMS: 50,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	next := func() wire.EventPayload {
		t.Helper()
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatal("event stream closed early")
			}
			return event
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for an event")
		}
		panic("unreachable")
	}
	if event := next(); event.Type != "spawned" || event.SessionID != info.ID || event.Command != "/bin/sh" {
		t.Fatalf("first event should announce the spawn: %+v", event)
	}
	if event := next(); event.Type != "idle" || event.SessionID != info.ID {
		t.Fatalf("expected idle once the greeting went quiet: %+v", event)
	}
	if err := c.Remove(info.ID); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	seen := map[string]bool{}
	for !seen["exited"] || !seen["removed"] {
		seen[next().Type] = true
	}
}

// syncBuffer keeps the audit writer race-free against the test's reads.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestAuditTrailRecordsSends(t *testing.T) {
	var audit syncBuffer
	c := startStack(t, WithAudit(log.New(&audit, "", 0)))

	open, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `sleep 60`},
		Cols:    80,
		Rows:    24,
		Peer:    true,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	closed, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `sleep 60`},
		Cols:    80,
		Rows:    24,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	if err := c.Send(open.ID, []byte("hello over there\r"), "sender-session"); err != nil {
		t.Fatalf("Send to peer session: %v", err)
	}
	if err := c.Send(closed.ID, []byte("psst\r"), "sender-session"); err == nil {
		t.Fatal("Send to non-peer session must fail")
	}
	if err := c.Input(open.ID, []byte("anon\r")); err != nil {
		t.Fatalf("unattributed Input: %v", err)
	}

	// The client call returns only after handle wrote the audit line, so
	// the trail is complete the moment the sends are.
	trail := audit.String()
	for _, want := range []string{
		"from=sender-session to=" + open.ID + " delivered (17 bytes): \"hello over there\\r\"",
		"from=sender-session to=" + closed.ID + " refused: no peer access",
		"from=unattributed to=" + open.ID + " delivered",
	} {
		if !strings.Contains(trail, want) {
			t.Fatalf("audit trail missing %q:\n%s", want, trail)
		}
	}
}

// A session's terminal can be moved by anyone — another dashboard, a
// full-screen attach — so every attachment hears about it: the new size
// arrives over the stream, ahead of the repaint it travels with, and a
// handler registered late still gets the last size.
func TestResizeNoticeReachesAttachedClients(t *testing.T) {
	c := startStack(t)
	info, err := c.Spawn(wire.Request{
		Command: "/bin/sh",
		Args:    []string{"-c", `printf landmark; while :; do sleep 0.1; done`},
		Cols:    80,
		Rows:    24,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	a, err := c.Attach(info.ID, 64)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer a.Close()

	sizes := make(chan [2]int, 4)
	a.OnResize(func(cols, rows int) { sizes <- [2]int{cols, rows} })

	if err := c.Resize(info.ID, 132, 43); err != nil {
		t.Fatalf("Resize: %v", err)
	}
	select {
	case size := <-sizes:
		if size != [2]int{132, 43} {
			t.Fatalf("resize notice = %v, want 132x43", size)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no resize notice reached the attachment")
	}
	// The repaint rides right behind the notice.
	drainUntil(t, a, "landmark")

	// Late registration replays the size the stream already carried.
	late, err := c.Attach(info.ID, 64)
	if err != nil {
		t.Fatalf("second Attach: %v", err)
	}
	defer late.Close()
	if err := c.Resize(info.ID, 100, 30); err != nil {
		t.Fatalf("second Resize: %v", err)
	}
	lateSizes := make(chan [2]int, 4)
	deadline := time.After(2 * time.Second)
	for {
		late.OnResize(func(cols, rows int) { lateSizes <- [2]int{cols, rows} })
		select {
		case size := <-lateSizes:
			if size != [2]int{100, 30} {
				t.Fatalf("late resize notice = %v, want 100x30", size)
			}
			return
		case <-deadline:
			t.Fatal("late handler never saw the pending resize")
		default:
			late.OnResize(nil)
			time.Sleep(10 * time.Millisecond)
		}
	}
}
