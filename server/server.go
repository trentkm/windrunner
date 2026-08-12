// Package server exposes an Engine over a listener, speaking the wire
// protocol. It has no opinion about daemonization; cmd/windrunner shows
// the usual shape.
package server

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/trentkm/windrunner"
	"github.com/trentkm/windrunner/wire"
)

// Serve accepts connections until the listener closes. Each connection
// declares itself with its first frame: control connections stay in a
// request loop, attach connections become one session's stream.
func Serve(engine *windrunner.Engine, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go serveConn(engine, conn)
	}
}

func serveConn(engine *windrunner.Engine, conn net.Conn) {
	defer conn.Close()
	for {
		frameType, payload, err := wire.ReadFrame(conn)
		if err != nil {
			return
		}
		switch frameType {
		case wire.FrameControl:
			var request wire.Request
			if err := json.Unmarshal(payload, &request); err != nil {
				respondError(conn, "malformed request: "+err.Error())
				return
			}
			if err := wire.WriteJSON(conn, wire.FrameResponse, handle(engine, request)); err != nil {
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
		default:
			respondError(conn, "unexpected frame before attach")
			return
		}
	}
}

func respondError(conn net.Conn, message string) {
	_ = wire.WriteJSON(conn, wire.FrameError, wire.ErrorPayload{Error: message})
}

func handle(engine *windrunner.Engine, request wire.Request) wire.Response {
	switch request.Op {
	case "spawn":
		s, err := engine.Spawn(windrunner.SpawnSpec{
			Command:    request.Command,
			Args:       request.Args,
			Dir:        request.Dir,
			Env:        request.Env,
			Cols:       request.Cols,
			Rows:       request.Rows,
			Scrollback: request.Scrollback,
			Metadata:   request.Metadata,
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
		if _, err := s.Write(request.Bytes); err != nil {
			return wire.Response{Error: err.Error()}
		}
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
		Metadata: s.Metadata(),
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
	snapshot, sub := s.Attach(request.Buffer)
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
		for chunk := range sub.Output() {
			if err := writeRaw(wire.FrameOutput, chunk); err != nil {
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
