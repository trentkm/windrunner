package client

import (
	"net"
	"testing"
	"time"

	"github.com/trentkm/windrunner/wire"
)

// pipeAttachment builds an Attachment on both ends of a pipe, skipping the
// dial: these tests are about how the read loop surfaces frames, not about
// the daemon that sends them.
func pipeAttachment(t *testing.T) (*Attachment, net.Conn) {
	t.Helper()
	ours, theirs := net.Pipe()
	a := &Attachment{
		conn:   ours,
		output: make(chan []byte, 8),
		exited: make(chan int, 1),
	}
	go a.readLoop()
	t.Cleanup(func() { a.Close() })
	return a, theirs
}

// TestResyncReachesTheHandler: a snapshot arriving after the attach-time
// one means the daemon replaced this attachment's backlog with exact
// state. It must reach the handler rather than the output stream — a
// replica that appended it would paint the terminal twice.
func TestResyncReachesTheHandler(t *testing.T) {
	a, daemon := pipeAttachment(t)

	resynced := make(chan wire.SnapshotPayload, 1)
	a.OnResync(func(state wire.SnapshotPayload) { resynced <- state })

	go func() {
		_ = wire.WriteJSON(daemon, wire.FrameSnapshot, wire.SnapshotPayload{
			Cols: 100, Rows: 30, ANSI: []byte("exact state"),
		})
		_ = wire.WriteFrame(daemon, wire.FrameOutput, []byte("and on from there"))
	}()

	select {
	case state := <-resynced:
		if state.Cols != 100 || state.Rows != 30 {
			t.Fatalf("resync size = %dx%d, want 100x30", state.Cols, state.Rows)
		}
		if string(state.ANSI) != "exact state" {
			t.Fatalf("resync ANSI = %q", state.ANSI)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the resync never reached the handler")
	}

	// The stream continues behind it, and the state never leaks into it.
	select {
	case chunk := <-a.Output():
		if string(chunk) != "and on from there" {
			t.Fatalf("output after resync = %q", chunk)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("output stopped after the resync")
	}
}

// TestResyncBeforeRegistrationIsReplayed: the handler is registered after
// the attachment exists, so a resync can beat it. Like OnResize, the one
// that arrived first is delivered on registration — dropping it would
// leave the replica silently stale.
func TestResyncBeforeRegistrationIsReplayed(t *testing.T) {
	a, daemon := pipeAttachment(t)

	if err := wire.WriteJSON(daemon, wire.FrameSnapshot, wire.SnapshotPayload{
		Cols: 80, Rows: 24, ANSI: []byte("missed it"),
	}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	resynced := make(chan wire.SnapshotPayload, 1)
	// The read loop may not have handled the frame yet; registering
	// repeatedly is how a caller with no other signal would do it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		a.OnResync(func(state wire.SnapshotPayload) { resynced <- state })
		select {
		case state := <-resynced:
			if string(state.ANSI) != "missed it" {
				t.Fatalf("replayed resync = %q", state.ANSI)
			}
			return
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("a resync that arrived before registration was dropped")
		}
	}
}
