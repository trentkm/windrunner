// Package server exposes an Engine over a listener, speaking the wire
// protocol. It has no opinion about daemonization; cmd/windrunner shows
// the usual shape.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trentkm/windrunner"
	"github.com/trentkm/windrunner/wire"
)

// Option adjusts how Serve runs.
type Option func(*config)

type config struct {
	audit *log.Logger
}

// WithAudit routes the audit trail to logger: one line per detached input
// op — delivered, refused, or failed — with the self-declared sender, the
// target, and the bytes. When sessions talk to each other, this is the
// transcript of who said what.
func WithAudit(logger *log.Logger) Option {
	return func(cfg *config) { cfg.audit = logger }
}

// Serve accepts connections until the listener closes. Each connection
// declares itself with its first frame: control connections stay in a
// request loop, attach connections become one session's stream.
func Serve(engine *windrunner.Engine, listener net.Listener, options ...Option) error {
	var cfg config
	for _, option := range options {
		option(&cfg)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go serveConn(engine, conn, cfg)
	}
}

func serveConn(engine *windrunner.Engine, conn net.Conn, cfg config) {
	defer conn.Close()
	for {
		frameType, payload, err := wire.ReadFrame(conn)
		if err != nil {
			return
		}
		switch frameType {
		case wire.FrameControl:
			request, err := decodeRequest(payload)
			if err != nil {
				respondError(conn, err.Error())
				return
			}
			if err := wire.WriteJSON(conn, wire.FrameResponse, handle(engine, request, cfg)); err != nil {
				return
			}
		case wire.FrameAttach:
			var request wire.AttachRequest
			if err := json.Unmarshal(payload, &request); err != nil {
				respondError(conn, "malformed attach: "+err.Error())
				return
			}
			// The connection belongs to the session now; serveAttach
			// runs it to the end.
			serveAttach(engine, conn, request)
			return
		case wire.FrameSubscribe:
			var request wire.SubscribeRequest
			if err := json.Unmarshal(payload, &request); err != nil {
				respondError(conn, "malformed subscribe: "+err.Error())
				return
			}
			// The connection is an event feed now; serveSubscribe runs
			// it to the end.
			serveSubscribe(engine, conn, request)
			return
		default:
			respondError(conn, "unexpected frame before attach")
			return
		}
	}
}

func respondError(conn net.Conn, message string) {
	_ = wire.WriteJSON(conn, wire.FrameError, wire.ErrorPayload{Error: message})
}

// decodeRequest refuses a request it does not fully understand.
//
// Ignoring an unknown field is the friendly-looking choice and the wrong
// one. A daemon outlives the client that started it — that is the whole
// point of a daemon — so a client newer than the daemon is the ordinary
// case, not the exotic one. Silently dropping the part it cannot read
// means spawning a session that is subtly not what was asked for: the
// environment a caller depended on, quietly absent, discovered later as
// something failing three layers away with no mention of a daemon.
//
// Refusing says which field and lets the caller say something useful —
// that the daemon is older than the build talking to it, and restarting
// it is the fix.
func decodeRequest(payload []byte) (wire.Request, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request wire.Request
	if err := decoder.Decode(&request); err != nil {
		if field, ok := unknownField(err); ok {
			return wire.Request{}, fmt.Errorf(
				"this daemon does not understand %q; it is older than the client "+
					"talking to it, and restarting it will fix that", field)
		}
		return wire.Request{}, fmt.Errorf("malformed request: %w", err)
	}
	return request, nil
}

// unknownField pulls the field name out of encoding/json's message, which
// says it in prose and nowhere else.
func unknownField(err error) (string, bool) {
	const marker = "unknown field "
	message := err.Error()
	index := strings.Index(message, marker)
	if index < 0 {
		return "", false
	}
	return strings.Trim(message[index+len(marker):], `"`), true
}

func handle(engine *windrunner.Engine, request wire.Request, cfg config) wire.Response {
	switch request.Op {
	case "spawn":
		s, err := engine.Spawn(windrunner.SpawnSpec{
			Command:     request.Command,
			Args:        request.Args,
			Dir:         request.Dir,
			Env:         request.Env,
			EnvOverride: request.EnvOverride,
			Cols:        request.Cols,
			Rows:        request.Rows,
			Scrollback:  request.Scrollback,
			IdleAfter:   time.Duration(request.IdleAfterMS) * time.Millisecond,
			Metadata:    request.Metadata,
			Peer:        request.Peer,
		})
		if err != nil {
			return wire.Response{Error: err.Error()}
		}
		info := describe(s)
		return wire.Response{OK: true, Session: &info}
	case "list":
		sessions := engine.Sessions()
		infos := make([]wire.SessionInfo, 0, len(sessions))
		for _, s := range sessions {
			infos = append(infos, describe(s))
		}
		return wire.Response{OK: true, Sessions: infos}
	case "info":
		s, ok := engine.Session(request.ID)
		if !ok {
			return wire.Response{Error: "no such session: " + request.ID}
		}
		info := describe(s)
		return wire.Response{OK: true, Session: &info}
	case "kill":
		s, ok := engine.Session(request.ID)
		if !ok {
			return wire.Response{Error: "no such session: " + request.ID}
		}
		if err := s.Kill(); err != nil {
			return wire.Response{Error: err.Error()}
		}
		return wire.Response{OK: true}
	case "remove":
		engine.Remove(request.ID)
		return wire.Response{OK: true}
	case "resize":
		s, ok := engine.Session(request.ID)
		if !ok {
			return wire.Response{Error: "no such session: " + request.ID}
		}
		if err := s.Resize(request.Cols, request.Rows); err != nil {
			return wire.Response{Error: err.Error()}
		}
		return wire.Response{OK: true}
	case "set_metadata":
		s, ok := engine.Session(request.ID)
		if !ok {
			return wire.Response{Error: "no such session: " + request.ID}
		}
		s.SetMetadata(request.Metadata)
		return wire.Response{OK: true}
	case "input":
		s, ok := engine.Session(request.ID)
		if !ok {
			return wire.Response{Error: "no such session: " + request.ID}
		}
		if !s.Peer() {
			cfg.auditSend(request, "refused: no peer access")
			return wire.Response{Error: "session " + request.ID + " does not accept peer input (opt in at spawn)"}
		}
		if _, err := s.Write(request.Bytes); err != nil {
			cfg.auditSend(request, "failed: "+err.Error())
			return wire.Response{Error: err.Error()}
		}
		cfg.auditSend(request, "delivered")
		return wire.Response{OK: true}
	case "snapshot":
		s, ok := engine.Session(request.ID)
		if !ok {
			return wire.Response{Error: "no such session: " + request.ID}
		}
		snapshot := s.Snapshot()
		return wire.Response{OK: true, Snapshot: &wire.SnapshotPayload{
			Cols: snapshot.Cols,
			Rows: snapshot.Rows,
			ANSI: snapshot.ANSI,
		}}
	default:
		return wire.Response{Error: "unknown op: " + request.Op}
	}
}

func describe(s *windrunner.Session) wire.SessionInfo {
	cols, rows := s.Size()
	return wire.SessionInfo{
		ID:       s.ID(),
		Alive:    s.Alive(),
		ExitCode: s.ExitCode(),
		Cols:     cols,
		Rows:     rows,
		Title:    s.Title(),
		Peer:     s.Peer(),
		Metadata: s.Metadata(),
	}
}

// auditTextLimit bounds how much of a send lands in the audit trail; the
// tail of anything longer is counted, not lost silently.
const auditTextLimit = 1024

func (cfg config) auditSend(request wire.Request, outcome string) {
	if cfg.audit == nil {
		return
	}
	from := request.From
	if from == "" {
		from = "unattributed"
	}
	text := request.Bytes
	truncated := ""
	if len(text) > auditTextLimit {
		truncated = " +" + strconv.Itoa(len(text)-auditTextLimit) + " bytes"
		text = text[:auditTextLimit]
	}
	cfg.audit.Printf("send from=%s to=%s %s (%d bytes): %q%s",
		from, request.ID, outcome, len(request.Bytes), text, truncated)
}

// serveSubscribe streams engine events over one connection: an ack first,
// then one Event frame per event until the client hangs up, the engine
// closes, or the subscriber lags.
func serveSubscribe(engine *windrunner.Engine, conn net.Conn, request wire.SubscribeRequest) {
	sub := engine.Subscribe(request.Buffer)
	defer sub.Close()
	if err := wire.WriteJSON(conn, wire.FrameResponse, wire.Response{OK: true}); err != nil {
		return
	}
	// The client speaks only by hanging up; notice when it does.
	go func() {
		for {
			if _, _, err := wire.ReadFrame(conn); err != nil {
				sub.Close()
				return
			}
		}
	}()
	for event := range sub.Events() {
		err := wire.WriteJSON(conn, wire.FrameEvent, wire.EventPayload{
			Type:      string(event.Type),
			SessionID: event.SessionID,
			Command:   event.Command,
			ExitCode:  event.ExitCode,
		})
		if err != nil {
			return
		}
	}
	if sub.Lagged() {
		respondError(conn, "event subscriber lagged; subscribe again")
	}
}

// serveAttach streams one session over one connection: snapshot first,
// then output frames until the stream ends, with input and resize frames
// flowing the other way. An Exited frame closes the story when the
// process is already gone or dies while attached.
func serveAttach(engine *windrunner.Engine, conn net.Conn, request wire.AttachRequest) {
	s, ok := engine.Session(request.ID)
	if !ok {
		respondError(conn, "no such session: "+request.ID)
		return
	}
	snapshot, sub := s.AttachWith(windrunner.AttachOptions{
		Buffer: request.Buffer,
		Resync: request.Resync,
	})
	defer sub.Close()

	// One writer at a time: the output pump and the exit notice race for
	// the connection.
	var writeMu sync.Mutex
	write := func(frameType wire.FrameType, payload any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return wire.WriteJSON(conn, frameType, payload)
	}
	writeRaw := func(frameType wire.FrameType, payload []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return wire.WriteFrame(conn, frameType, payload)
	}

	if err := write(wire.FrameSnapshot, wire.SnapshotPayload{
		Cols: snapshot.Cols,
		Rows: snapshot.Rows,
		ANSI: snapshot.ANSI,
	}); err != nil {
		return
	}

	// detached says the client side ended things; without it, the writer
	// could not tell "stream closed because the process died" (announce
	// the exit — the PTY hits EOF a beat before Wait records the code, so
	// this must wait, not poll) from "client went away" (just leave).
	detached := make(chan struct{})
	var detachOnce sync.Once
	detach := func() { detachOnce.Do(func() { close(detached) }) }

	done := make(chan struct{})
	go func() {
		defer close(done)
		for message := range sub.Output() {
			if message.Resync != nil {
				// The client fell behind; its backlog is gone and this
				// state stands in for it. Same frame as the attach-time
				// snapshot: a resyncing client already knows to replace
				// its replica when one arrives.
				if err := write(wire.FrameSnapshot, wire.SnapshotPayload{
					Cols: message.Resync.Cols,
					Rows: message.Resync.Rows,
					ANSI: message.Resync.ANSI,
				}); err != nil {
					conn.Close()
					return
				}
				continue
			}
			if message.Resize != nil {
				// The size travels ahead of the repaint it arrived with,
				// so a client resizes its replica and then paints.
				if err := write(wire.FrameResize, wire.ResizePayload{
					Cols: message.Resize.Cols,
					Rows: message.Resize.Rows,
				}); err != nil {
					conn.Close()
					return
				}
			}
			if err := writeRaw(wire.FrameOutput, message.Bytes); err != nil {
				conn.Close()
				return
			}
		}
		select {
		case <-s.Done():
			_ = write(wire.FrameExited, wire.ExitedPayload{ExitCode: s.ExitCode()})
		case <-detached:
		}
	}()

	for {
		frameType, payload, err := wire.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				conn.Close()
			}
			detach()
			sub.Close()
			<-done
			return
		}
		switch frameType {
		case wire.FrameInput:
			if _, err := s.Write(payload); err != nil {
				detach()
				sub.Close()
				<-done
				return
			}
		case wire.FrameResize:
			var resize wire.ResizePayload
			if err := json.Unmarshal(payload, &resize); err == nil {
				_ = s.Resize(resize.Cols, resize.Rows)
			}
		default:
			// Tolerate unknown frames from newer clients.
		}
	}
}
