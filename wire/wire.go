// Package wire is windrunner's socket protocol: length-prefixed frames
// over a unix socket. Three connection styles share one framing:
//
//   - A control connection carries JSON requests and responses (spawn,
//     list, kill, remove, resize, metadata). Every request is answered by
//     a Response and the connection stays open, including when the answer
//     is a refusal — a request the daemon cannot read or cannot carry out
//     is one bad request, not a broken stream.
//   - An attach connection is dedicated to one session: the client sends
//     Attach, the server answers with a Snapshot, and from then on the
//     server streams Output frames while the client sends Input and
//     Resize. Closing the connection is detaching. A client that asked to
//     resync may also receive a later Snapshot frame, which replaces its
//     replica wholesale (see AttachRequest.Resync).
//   - A subscribe connection is a feed of engine events: the client sends
//     Subscribe, the server acks with a Response, and from then on the
//     server streams Event frames — session lifecycle and idle/busy
//     transitions. Closing the connection unsubscribes.
//
// Frames are: 1 type byte, 4 length bytes (big endian), payload. Output
// and Input payloads are raw terminal bytes; everything else is JSON.
package wire

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

type FrameType byte

const (
	FrameControl   FrameType = 1  // JSON Request
	FrameResponse  FrameType = 2  // JSON Response
	FrameAttach    FrameType = 3  // JSON AttachRequest
	FrameSnapshot  FrameType = 4  // JSON SnapshotPayload
	FrameOutput    FrameType = 5  // raw terminal output
	FrameInput     FrameType = 6  // raw terminal input
	FrameResize    FrameType = 7  // JSON ResizePayload
	FrameExited    FrameType = 8  // JSON ExitedPayload
	FrameError     FrameType = 9  // JSON ErrorPayload
	FrameSubscribe FrameType = 10 // JSON SubscribeRequest
	FrameEvent     FrameType = 11 // JSON EventPayload
)

// MaxFrame bounds a single frame; snapshots of deep scrollback are the
// largest legitimate payload.
const MaxFrame = 32 << 20

// Request is every control operation; Op selects which fields matter.
type Request struct {
	Op string `json:"op"` // spawn, list, info, kill, remove, resize, set_metadata, input, snapshot

	// spawn
	Command string   `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
	Dir     string   `json:"dir,omitempty"`
	Env     []string `json:"env,omitempty"`
	// spawn: variables applied on top of Env, or on top of the daemon's
	// own environment when Env is empty. A client that is not on the
	// daemon's machine sends its variables this way — its environ
	// describes a different host.
	EnvOverride []string          `json:"env_override,omitempty"`
	Scrollback  int               `json:"scrollback,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	// spawn: quiet window in milliseconds behind idle/busy events; 0
	// means the engine default.
	IdleAfterMS int `json:"idle_after_ms,omitempty"`
	// spawn: opt the session into peer input. Sessions that did not opt
	// in refuse the input op — sending into a session is remote code
	// execution as far as its process is concerned, so it is off unless
	// asked for.
	Peer bool `json:"peer,omitempty"`

	// spawn, resize
	Cols int `json:"cols,omitempty"`
	Rows int `json:"rows,omitempty"`

	// info, kill, remove, resize, set_metadata, input, snapshot
	ID string `json:"id,omitempty"`

	// input: terminal input bytes delivered without an attachment —
	// automation writes this way; attachments are for watching.
	Bytes []byte `json:"bytes,omitempty"`

	// input: self-declared sender identity for the daemon's audit trail,
	// typically the sending session's own ID (WINDRUNNER_SESSION). The
	// daemon records it verbatim; it grants nothing.
	From string `json:"from,omitempty"`
}

// SessionInfo is what the daemon says about a session.
type SessionInfo struct {
	ID       string            `json:"id"`
	Command  string            `json:"command"`
	Alive    bool              `json:"alive"`
	ExitCode int               `json:"exit_code"`
	Cols     int               `json:"cols"`
	Rows     int               `json:"rows"`
	Title    string            `json:"title,omitempty"`
	Peer     bool              `json:"peer,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Response struct {
	OK       bool             `json:"ok"`
	Error    string           `json:"error,omitempty"`
	Session  *SessionInfo     `json:"session,omitempty"`
	Sessions []SessionInfo    `json:"sessions,omitempty"`
	Snapshot *SnapshotPayload `json:"snapshot,omitempty"`
}

type AttachRequest struct {
	ID string `json:"id"`
	// Buffer is the subscriber's chunk buffer; falling behind it means
	// being dropped and re-attaching.
	Buffer int `json:"buffer,omitempty"`
	// Resync asks to stay attached when the client falls behind: instead
	// of being dropped, it receives another Snapshot frame carrying the
	// terminal's exact state at that moment, and the stream continues
	// from there. A client that asks for it must treat a mid-stream
	// Snapshot as a replacement for its replica, not as more output.
	Resync bool `json:"resync,omitempty"`
	// Cols/Rows, both at least 2, state this viewer's geometry. The
	// session follows the newest statement among its attached viewers
	// and retires this one when the attachment ends; the statement lands
	// before the snapshot, so the state that answers the attach is
	// already wrapped for it. Zero states nothing — the terminal is
	// shared, and a viewer without a layout has no size to state.
	Cols int `json:"cols,omitempty"`
	Rows int `json:"rows,omitempty"`
}

type SnapshotPayload struct {
	Cols int    `json:"cols"`
	Rows int    `json:"rows"`
	ANSI []byte `json:"ansi"`
}

type SubscribeRequest struct {
	// Buffer is the subscriber's event buffer; falling behind it means
	// being dropped and re-subscribing.
	Buffer int `json:"buffer,omitempty"`
}

// EventPayload is one engine event: spawned, exited, removed, idle, busy.
type EventPayload struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Command   string `json:"command,omitempty"`   // spawned
	ExitCode  int    `json:"exit_code,omitempty"` // exited
}

type ResizePayload struct {
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

type ExitedPayload struct {
	ExitCode int `json:"exit_code"`
}

// ErrorPayload reports a failure that has no Response to live in: an
// attach or subscribe that never started, or a first frame that made no
// sense. A refused control request is a Response with OK false instead,
// so that one connection style has one failure shape.
type ErrorPayload struct {
	Error string `json:"error"`
}

// WriteFrame emits one frame. Concurrent writers must serialize
// themselves.
func WriteFrame(w io.Writer, frameType FrameType, payload []byte) error {
	if len(payload) > MaxFrame {
		return fmt.Errorf("wire: frame of %d bytes exceeds limit", len(payload))
	}
	var header [5]byte
	header[0] = byte(frameType)
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// WriteJSON marshals payload and emits it as one frame.
func WriteJSON(w io.Writer, frameType FrameType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wire: marshal frame: %w", err)
	}
	return WriteFrame(w, frameType, raw)
}

// ReadFrame reads one frame, allocating its payload.
func ReadFrame(r io.Reader) (FrameType, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length > MaxFrame {
		return 0, nil, fmt.Errorf("wire: frame of %d bytes exceeds limit", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return FrameType(header[0]), payload, nil
}
