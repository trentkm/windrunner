package server

import (
	"testing"
	"time"

	"github.com/trentkm/windrunner/client"
	"github.com/trentkm/windrunner/wire"
)

// The failure this model exists to end: a viewer attaches, moves the
// shared terminal, and leaves — and its size used to stay behind, owning
// a terminal nobody was watching at that geometry (stormlight#155). Over
// the wire, a statement must arrive with the attach, follow the
// attachment's resizes, and retire the moment the connection ends.
func TestAViewersSizeRetiresWithItsConnection(t *testing.T) {
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

	wantSize := func(cols, rows int, story string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			current, err := c.Info(info.ID)
			if err != nil {
				t.Fatalf("Info: %v", err)
			}
			if current.Cols == cols && current.Rows == rows {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("size %dx%d, want %dx%d: %s",
					current.Cols, current.Rows, cols, rows, story)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// The dashboard: attaches stating its pane, and stays.
	dashboard, err := c.AttachWith(info.ID, client.AttachOptions{
		Resync: true, Cols: 200, Rows: 50,
	})
	if err != nil {
		t.Fatalf("attach dashboard: %v", err)
	}
	defer dashboard.Close()
	if snapshot := dashboard.Snapshot(); snapshot.Cols != 200 || snapshot.Rows != 50 {
		t.Fatalf("dashboard snapshot %dx%d, want 200x50: the statement "+
			"lands before the state that answers the attach",
			snapshot.Cols, snapshot.Rows)
	}
	wantSize(200, 50, "the dashboard's statement to take the terminal")

	// The visitor: a second viewer at another geometry. Its statement is
	// newer, so it wins — and a resize on its own connection stays its
	// statement, not a command that outlives it.
	visitor, err := c.AttachWith(info.ID, client.AttachOptions{
		Resync: true, Cols: 98, Rows: 38,
	})
	if err != nil {
		t.Fatalf("attach visitor: %v", err)
	}
	wantSize(98, 38, "the visitor's statement to take the terminal")
	if err := visitor.Resize(110, 42); err != nil {
		t.Fatalf("visitor resize: %v", err)
	}
	wantSize(110, 42, "the visitor's resize to follow its attachment")

	// The visitor hangs up. Nobody re-asserts anything: the daemon
	// itself settles the terminal on the viewer still watching.
	visitor.Close()
	wantSize(200, 50, "the terminal to come home to the dashboard")
}
