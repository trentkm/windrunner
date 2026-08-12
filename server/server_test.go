package server

import (
	"net"
	"path/filepath"
	"regexp"
	"strings"
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

func startStack(t *testing.T) *client.Client {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "wr.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	engine := windrunner.NewEngine()
	t.Cleanup(engine.Close)
	go Serve(engine, listener)
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
