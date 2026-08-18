package server

import (
	"io"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trentkm/windrunner/client"
	"github.com/trentkm/windrunner/wire"
)

// bridgeDialer reaches the daemon the way a tunnel does: what the client
// gets is an in-memory pipe, spliced byte for byte onto a real socket
// connection by a pump in the middle. That is the topology of an SSH
// bridge — a remote process joining its stdio to the daemon's socket —
// with the process and the network taken out, so what it exercises is the
// seam rather than the transport. It is also the harsher of the two: a
// net.Pipe is unbuffered, so every frame has to be read before the next
// one can be written.
func bridgeDialer(socket string) client.Dialer {
	return func() (net.Conn, error) {
		far, err := net.Dial("unix", socket)
		if err != nil {
			return nil, err
		}
		near, bridge := net.Pipe()
		go func() {
			io.Copy(far, bridge)
			// The client hung up: pass the close on, or the daemon holds
			// an attachment nobody is reading.
			far.Close()
		}()
		go func() {
			io.Copy(bridge, far)
			bridge.Close()
		}()
		return near, nil
	}
}

// counting wraps a dialer to record how often it was asked for a
// connection.
func counting(opened *atomic.Int64, dial client.Dialer) client.Dialer {
	return func() (net.Conn, error) {
		opened.Add(1)
		return dial()
	}
}

// TestEveryConnectionFollowsTheDialer runs one story over both
// transports. A client makes three kinds of connection — the control
// plane, the event feed, an attachment — and every one has to come from
// the dialer. One that reached for the socket directly would pass every
// test on this machine and then, over a tunnel, work until the first
// attach and quietly dial a path that only exists on the far side. So the
// count is asserted, not just the behavior.
func TestEveryConnectionFollowsTheDialer(t *testing.T) {
	transports := []struct {
		name   string
		dialer func(socket string) client.Dialer
	}{
		{"unix", client.UnixDialer},
		{"bridged", bridgeDialer},
	}

	for _, transport := range transports {
		t.Run(transport.name, func(t *testing.T) {
			socket := startDaemon(t)
			var opened atomic.Int64
			c, err := client.DialWith(counting(&opened, transport.dialer(socket)))
			if err != nil {
				t.Fatalf("DialWith: %v", err)
			}
			t.Cleanup(func() { c.Close() })

			// The event feed takes its own connection.
			stream, err := c.Subscribe(64)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			defer stream.Close()

			// Control calls share the client's.
			info, err := c.Spawn(wire.Request{
				Command:  "/bin/sh",
				Args:     []string{"-c", `printf 'tunnel open\n'; read line; printf 'got:%s\n' "$line"; exit 7`},
				Cols:     80,
				Rows:     24,
				Metadata: map[string]string{"name": "tunnelled"},
			})
			if err != nil {
				t.Fatalf("Spawn: %v", err)
			}
			if sessions, err := c.List(); err != nil || len(sessions) != 1 ||
				sessions[0].Metadata["name"] != "tunnelled" {
				t.Fatalf("List: %v, %+v", err, sessions)
			}

			// An attachment takes a third.
			a, err := c.Attach(info.ID, 64)
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			defer a.Close()
			if seen := stripANSI(string(a.Snapshot().ANSI)); !strings.Contains(seen, "tunnel open") {
				drainUntil(t, a, "tunnel open")
			}

			// Resize is the daemon pushing something other than output
			// down the attachment.
			if err := c.Resize(info.ID, 100, 30); err != nil {
				t.Fatalf("Resize: %v", err)
			}
			if size := awaitResize(t, a); size != [2]int{100, 30} {
				t.Fatalf("resize arrived as %v", size)
			}

			// Input goes back up the same connection, and the reply
			// comes down it.
			if err := a.Write([]byte("through the pipe\r")); err != nil {
				t.Fatalf("Write: %v", err)
			}
			drainUntil(t, a, "got:through the pipe")

			// The child's own exit code, over the attachment.
			select {
			case code := <-a.Exited():
				if code != 7 {
					t.Fatalf("exit code %d, want 7", code)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("exit never reached the attachment")
			}

			// State outlives the attachment, and reading it is a control
			// call again.
			snapshot, err := c.Snapshot(info.ID)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if replayed := stripANSI(string(snapshot.ANSI)); !strings.Contains(replayed, "got:through the pipe") {
				t.Fatalf("snapshot lost the story:\n%s", replayed)
			}

			// And the event feed carried the same session's arc.
			deadline := time.After(5 * time.Second)
			var types []string
			for !slices.Contains(types, "exited") {
				select {
				case event, ok := <-stream.Events():
					if !ok {
						t.Fatalf("event stream closed early; saw %v", types)
					}
					if event.SessionID != info.ID {
						t.Fatalf("event for a session nobody spawned: %+v", event)
					}
					types = append(types, event.Type)
				case <-deadline:
					t.Fatalf("timed out waiting for the exit event; saw %v", types)
				}
			}
			if types[0] != "spawned" {
				t.Fatalf("first event should announce the spawn; saw %v", types)
			}

			// Control plane, event feed, attachment: three, and no
			// connection made behind the dialer's back.
			if got := opened.Load(); got != 3 {
				t.Fatalf("dialer opened %d connections, want 3 (control, events, attach)", got)
			}
		})
	}
}
