package client

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/term"
)

// InteractiveResult says how an Interactive attachment ended.
type InteractiveResult int

const (
	// Detached: the user pressed the detach key; the session runs on.
	Detached InteractiveResult = iota
	// SessionExited: the process ended while attached.
	SessionExited
	// ConnectionLost: the daemon went away.
	ConnectionLost
)

// Interactive runs a full-terminal attachment: raw mode on the caller's
// terminal, input forwarded (detachKey excepted), output written through,
// resizes followed. It returns when the user detaches, the session's
// process exits, or the connection drops — restoring the terminal either
// way. This is the loop the windrunner CLI uses; it is exported because
// every embedding product eventually wants an "open the real terminal"
// escape hatch.
func (c *Client) Interactive(id string, detachKey byte) (InteractiveResult, error) {
	stdin := int(os.Stdin.Fd())
	if !term.IsTerminal(stdin) {
		return ConnectionLost, fmt.Errorf("windrunner: interactive attach needs a terminal")
	}
	// Size the session to this terminal before the snapshot is taken, so
	// it arrives pre-wrapped for the screen it is about to fill.
	if cols, rows, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		_ = c.Resize(id, cols, rows)
	}
	attachment, err := c.Attach(id, 256)
	if err != nil {
		return ConnectionLost, err
	}
	defer attachment.Close()

	previous, err := term.MakeRaw(stdin)
	if err != nil {
		return ConnectionLost, err
	}
	defer term.Restore(stdin, previous)

	snapshot := attachment.Snapshot()
	os.Stdout.WriteString("\x1b[2J\x1b[H")
	os.Stdout.Write(snapshot.ANSI)

	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	defer signal.Stop(winch)

	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		buf := make([]byte, 4096)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				for index, value := range chunk {
					if value == detachKey {
						if index > 0 {
							attachment.Write(chunk[:index])
						}
						return
					}
				}
				if attachment.Write(chunk) != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-inputDone:
			return Detached, nil
		case <-winch:
			if cols, rows, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
				_ = attachment.Resize(cols, rows)
			}
		case code := <-attachment.Exited():
			_ = code
			return SessionExited, nil
		case chunk, ok := <-attachment.Output():
			if !ok {
				return ConnectionLost, nil
			}
			os.Stdout.Write(chunk)
		}
	}
}
