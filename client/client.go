// Package client speaks the wire protocol to a windrunner daemon: control
// calls over one connection, attachments over one dedicated connection
// each.
package client

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/trentkm/windrunner/wire"
)

// Client is a control-plane connection to a daemon. Safe for concurrent
// use; calls are serialized on the wire.
type Client struct {
	socketPath string

	mu   sync.Mutex
	conn net.Conn
}

// Dial connects to a daemon's socket.
func Dial(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{socketPath: socketPath, conn: conn}, nil
}

// EnsureDaemon dials the socket, starting the daemon if nothing answers.
// daemonArgv is the command that runs it (typically the calling binary
// with a "daemon" subcommand); it is started detached and given until the
// timeout to open the socket.
func EnsureDaemon(socketPath string, daemonArgv []string, timeout time.Duration) (*Client, error) {
	if c, err := Dial(socketPath); err == nil {
		return c, nil
	}
	if len(daemonArgv) == 0 {
		return nil, fmt.Errorf("windrunner: no daemon at %s and no command to start one", socketPath)
	}
	// A dead socket file from a previous daemon refuses connections until
	// removed.
	os.Remove(socketPath)
	cmd := exec.Command(daemonArgv[0], daemonArgv[1:]...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("windrunner: start daemon: %w", err)
	}
	// The daemon owns its own life from here; reap it if it ever exits.
	go cmd.Wait()

	deadline := time.Now().Add(timeout)
	for {
		if c, err := Dial(socketPath); err == nil {
			return c, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("windrunner: daemon did not open %s within %s", socketPath, timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}

// callTimeout bounds every control call: a daemon that cannot answer in
// this long is a daemon to report broken, not to wait on.
const callTimeout = 10 * time.Second

func (c *Client) call(request wire.Request) (wire.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetDeadline(time.Now().Add(callTimeout))
	defer c.conn.SetDeadline(time.Time{})
	if err := wire.WriteJSON(c.conn, wire.FrameControl, request); err != nil {
		return wire.Response{}, err
	}
	frameType, payload, err := wire.ReadFrame(c.conn)
	if err != nil {
		return wire.Response{}, err
	}
	if frameType == wire.FrameError {
		var failure wire.ErrorPayload
		_ = json.Unmarshal(payload, &failure)
		return wire.Response{}, fmt.Errorf("windrunner: %s", failure.Error)
	}
	var response wire.Response
	if err := json.Unmarshal(payload, &response); err != nil {
		return wire.Response{}, fmt.Errorf("windrunner: malformed response: %w", err)
	}
	if !response.OK {
		return response, fmt.Errorf("windrunner: %s", response.Error)
	}
	return response, nil
}

// Spawn starts a session and reports it.
func (c *Client) Spawn(request wire.Request) (wire.SessionInfo, error) {
	request.Op = "spawn"
	response, err := c.call(request)
	if err != nil {
		return wire.SessionInfo{}, err
	}
	return *response.Session, nil
}

// List reports every session.
func (c *Client) List() ([]wire.SessionInfo, error) {
	response, err := c.call(wire.Request{Op: "list"})
	if err != nil {
		return nil, err
	}
	return response.Sessions, nil
}

// Info reports one session.
func (c *Client) Info(id string) (wire.SessionInfo, error) {
	response, err := c.call(wire.Request{Op: "info", ID: id})
	if err != nil {
		return wire.SessionInfo{}, err
	}
	return *response.Session, nil
}

// Kill ends a session's process; its state stays until Remove.
func (c *Client) Kill(id string) error {
	_, err := c.call(wire.Request{Op: "kill", ID: id})
	return err
}

// Remove discards a session entirely.
func (c *Client) Remove(id string) error {
	_, err := c.call(wire.Request{Op: "remove", ID: id})
	return err
}

// Resize moves a session's terminal.
func (c *Client) Resize(id string, cols, rows int) error {
	_, err := c.call(wire.Request{Op: "resize", ID: id, Cols: cols, Rows: rows})
	return err
}

// SetMetadata replaces a session's metadata.
func (c *Client) SetMetadata(id string, metadata map[string]string) error {
	_, err := c.call(wire.Request{Op: "set_metadata", ID: id, Metadata: metadata})
	return err
}

// Input delivers terminal input without an attachment — the way
// automation writes; attachments are for watching. The daemon refuses it
// for sessions that were not spawned with peer access.
func (c *Client) Input(id string, data []byte) error {
	return c.Send(id, data, "")
}

// Send is Input with attribution: from names the sender in the daemon's
// audit trail, typically the sending session's own WINDRUNNER_SESSION.
// It is recorded verbatim and grants nothing.
func (c *Client) Send(id string, data []byte, from string) error {
	_, err := c.call(wire.Request{Op: "input", ID: id, Bytes: data, From: from})
	return err
}

// Snapshot reads a session's exact terminal state without attaching.
func (c *Client) Snapshot(id string) (wire.SnapshotPayload, error) {
	response, err := c.call(wire.Request{Op: "snapshot", ID: id})
	if err != nil {
		return wire.SnapshotPayload{}, err
	}
	return *response.Snapshot, nil
}

// EventStream is a live feed of engine events, on its own connection.
type EventStream struct {
	conn   net.Conn
	events chan wire.EventPayload
	once   sync.Once
}

// Subscribe opens a dedicated connection to the daemon's event feed:
// session lifecycle (spawned, exited, removed) and activity (idle, busy).
// It returns once the daemon has acked, so no event after that is missed.
func (c *Client) Subscribe(buffer int) (*EventStream, error) {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return nil, err
	}
	if err := wire.WriteJSON(conn, wire.FrameSubscribe, wire.SubscribeRequest{Buffer: buffer}); err != nil {
		conn.Close()
		return nil, err
	}
	frameType, payload, err := wire.ReadFrame(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if frameType == wire.FrameError {
		var failure wire.ErrorPayload
		_ = json.Unmarshal(payload, &failure)
		conn.Close()
		return nil, fmt.Errorf("windrunner: %s", failure.Error)
	}
	if frameType != wire.FrameResponse {
		conn.Close()
		return nil, fmt.Errorf("windrunner: expected subscribe ack, got frame %d", frameType)
	}
	stream := &EventStream{conn: conn, events: make(chan wire.EventPayload, 64)}
	go stream.readLoop()
	return stream, nil
}

// Events yields engine events. The channel closes when the stream ends:
// Close, a daemon gone away, or the subscription dropped for lagging.
func (stream *EventStream) Events() <-chan wire.EventPayload { return stream.events }

// Close unsubscribes.
func (stream *EventStream) Close() {
	stream.once.Do(func() { stream.conn.Close() })
}

func (stream *EventStream) readLoop() {
	defer close(stream.events)
	for {
		frameType, payload, err := wire.ReadFrame(stream.conn)
		if err != nil {
			return
		}
		if frameType != wire.FrameEvent {
			// An error frame (lagged) or an unknown future frame; either
			// way the feed is over or unintelligible — end it.
			return
		}
		var event wire.EventPayload
		if err := json.Unmarshal(payload, &event); err != nil {
			return
		}
		stream.events <- event
	}
}

// Attachment is one live view of one session, on its own connection.
type Attachment struct {
	conn     net.Conn
	snapshot wire.SnapshotPayload
	output   chan []byte
	exited   chan int

	writeMu sync.Mutex
	once    sync.Once

	resizeMu      sync.Mutex
	onResize      func(cols, rows int)
	pendingResize *wire.ResizePayload
}

// Attach opens a dedicated connection to one session and returns after
// the snapshot has arrived; Output then carries everything since it.
func (c *Client) Attach(id string, buffer int) (*Attachment, error) {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return nil, err
	}
	if err := wire.WriteJSON(conn, wire.FrameAttach, wire.AttachRequest{ID: id, Buffer: buffer}); err != nil {
		conn.Close()
		return nil, err
	}
	frameType, payload, err := wire.ReadFrame(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if frameType == wire.FrameError {
		var failure wire.ErrorPayload
		_ = json.Unmarshal(payload, &failure)
		conn.Close()
		return nil, fmt.Errorf("windrunner: %s", failure.Error)
	}
	if frameType != wire.FrameSnapshot {
		conn.Close()
		return nil, fmt.Errorf("windrunner: expected snapshot, got frame %d", frameType)
	}
	a := &Attachment{
		conn:   conn,
		output: make(chan []byte, 64),
		exited: make(chan int, 1),
	}
	if err := json.Unmarshal(payload, &a.snapshot); err != nil {
		conn.Close()
		return nil, fmt.Errorf("windrunner: malformed snapshot: %w", err)
	}
	go a.readLoop()
	return a, nil
}

// Snapshot is the session's exact state at attach time.
func (a *Attachment) Snapshot() wire.SnapshotPayload { return a.snapshot }

// Output carries raw terminal bytes since the snapshot; it closes when
// the attachment ends.
func (a *Attachment) Output() <-chan []byte { return a.output }

// OnResize registers the handler for session resizes that arrive over the
// stream — the session's terminal moved, whoever moved it. The handler
// runs on the read loop, ahead of the repaint bytes the resize travels
// with; a resize that arrived before registration is delivered
// immediately.
func (a *Attachment) OnResize(handler func(cols, rows int)) {
	a.resizeMu.Lock()
	pending := a.pendingResize
	a.pendingResize = nil
	a.onResize = handler
	a.resizeMu.Unlock()
	if pending != nil && handler != nil {
		handler(pending.Cols, pending.Rows)
	}
}

func (a *Attachment) handleResize(payload wire.ResizePayload) {
	a.resizeMu.Lock()
	handler := a.onResize
	if handler == nil {
		a.pendingResize = &payload
	}
	a.resizeMu.Unlock()
	if handler != nil {
		handler(payload.Cols, payload.Rows)
	}
}

// Exited yields the process's exit code if it ends while attached.
func (a *Attachment) Exited() <-chan int { return a.exited }

// Write sends input bytes to the session.
func (a *Attachment) Write(p []byte) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return wire.WriteFrame(a.conn, wire.FrameInput, p)
}

// Resize moves the session's terminal.
func (a *Attachment) Resize(cols, rows int) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return wire.WriteJSON(a.conn, wire.FrameResize, wire.ResizePayload{Cols: cols, Rows: rows})
}

// Close detaches. The session is untouched.
func (a *Attachment) Close() {
	a.once.Do(func() { a.conn.Close() })
}

func (a *Attachment) readLoop() {
	defer close(a.output)
	for {
		frameType, payload, err := wire.ReadFrame(a.conn)
		if err != nil {
			return
		}
		switch frameType {
		case wire.FrameOutput:
			a.output <- payload
		case wire.FrameResize:
			var moved wire.ResizePayload
			if err := json.Unmarshal(payload, &moved); err == nil {
				a.handleResize(moved)
			}
		case wire.FrameExited:
			var exited wire.ExitedPayload
			if err := json.Unmarshal(payload, &exited); err == nil {
				select {
				case a.exited <- exited.ExitCode:
				default:
				}
			}
		}
	}
}
