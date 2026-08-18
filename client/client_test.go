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
		output: make(chan Message, 64),
		exited: make(chan int, 1),
	}
	go a.readLoop()
	t.Cleanup(func() { a.Close() })
	return a, theirs
}

// TestResyncKeepsItsPlaceInTheStream is the ordering contract, and the
// reason a resync travels in the stream rather than through a callback.
//
// A resync only ever reaches a client that is behind — one with output
// already queued. Deliver the state by a side channel and that queued
// output arrives after the state that superseded it, leaving the replica
// permanently wrong with nothing left to correct it. Here the daemon
// sends output, then state, then more output, while the reader is slow;
// what comes out must be in exactly that order.
func TestResyncKeepsItsPlaceInTheStream(t *testing.T) {
	a, daemon := pipeAttachment(t)

	go func() {
		for i := 0; i < 10; i++ {
			_ = wire.WriteFrame(daemon, wire.FrameOutput, []byte("old"))
		}
		_ = wire.WriteJSON(daemon, wire.FrameSnapshot, wire.SnapshotPayload{
			Cols: 100, Rows: 30, ANSI: []byte("exact state"),
		})
		_ = wire.WriteFrame(daemon, wire.FrameOutput, []byte("new"))
	}()

	// Read late, the way a client that fell behind does.
	time.Sleep(100 * time.Millisecond)

	var order []string
	deadline := time.After(5 * time.Second)
	for len(order) < 12 {
		select {
		case message, ok := <-a.Output():
			if !ok {
				t.Fatalf("stream closed after %v", order)
			}
			switch {
			case message.Resync != nil:
				order = append(order, "RESYNC")
				if message.Bytes != nil {
					t.Fatalf("a resync carries state alone, not %q", message.Bytes)
				}
				if message.Resync.Cols != 100 || message.Resync.Rows != 30 {
					t.Fatalf("resync size = %dx%d, want 100x30",
						message.Resync.Cols, message.Resync.Rows)
				}
			default:
				order = append(order, string(message.Bytes))
			}
		case <-deadline:
			t.Fatalf("timed out; got %v", order)
		}
	}

	for index, want := range []string{
		"old", "old", "old", "old", "old", "old", "old", "old", "old", "old",
		"RESYNC", "new",
	} {
		if order[index] != want {
			t.Fatalf("stream arrived out of order:\n got %v\nwant the resync at index 10", order)
		}
	}
}

// TestResizeTravelsInTheStream: the size arrives as its own message, in
// the position the daemon put it — just ahead of the repaint it belongs
// to.
func TestResizeTravelsInTheStream(t *testing.T) {
	a, daemon := pipeAttachment(t)

	go func() {
		_ = wire.WriteJSON(daemon, wire.FrameResize, wire.ResizePayload{Cols: 120, Rows: 40})
		_ = wire.WriteFrame(daemon, wire.FrameOutput, []byte("repaint"))
	}()

	message := next(t, a)
	if message.Resize == nil {
		t.Fatalf("first message was %#v, want a resize", message)
	}
	if message.Resize.Cols != 120 || message.Resize.Rows != 40 {
		t.Fatalf("resize = %dx%d, want 120x40", message.Resize.Cols, message.Resize.Rows)
	}
	if message := next(t, a); string(message.Bytes) != "repaint" {
		t.Fatalf("second message = %#v, want the repaint", message)
	}
}

// TestMalformedSnapshotEndsTheStream: a resync that cannot be parsed
// cannot be skipped. Everything after it would be appended to a replica
// this state was meant to replace, so the honest answer is to stop.
func TestMalformedSnapshotEndsTheStream(t *testing.T) {
	a, daemon := pipeAttachment(t)

	go func() {
		_ = wire.WriteFrame(daemon, wire.FrameSnapshot, []byte("{not json"))
		_ = wire.WriteFrame(daemon, wire.FrameOutput, []byte("would corrupt"))
	}()

	select {
	case message, ok := <-a.Output():
		if ok {
			t.Fatalf("stream continued past a malformed resync: %#v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream neither closed nor delivered anything")
	}
}

func next(t *testing.T, a *Attachment) Message {
	t.Helper()
	select {
	case message, ok := <-a.Output():
		if !ok {
			t.Fatal("stream closed early")
		}
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
	panic("unreachable")
}
