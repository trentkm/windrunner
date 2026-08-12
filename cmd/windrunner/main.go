// Command windrunner is the reference client and daemon: persistent
// terminal sessions you can create, list, and attach to — a minimal
// screen alternative that doubles as a demo of the library.
//
//	windrunner new [-name label] -- command args...
//	windrunner ls
//	windrunner attach <id-prefix>
//	windrunner kill <id-prefix>
//	windrunner rm <id-prefix>
//	windrunner daemon        (usually started for you)
//
// Detach from an attached session with ctrl+q.
package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/trentkm/windrunner"
	"github.com/trentkm/windrunner/client"
	"github.com/trentkm/windrunner/server"
	"github.com/trentkm/windrunner/wire"
)

const detachKey = 0x11 // ctrl+q

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "daemon":
		err = runDaemon()
	case "new":
		err = runNew(os.Args[2:])
	case "ls":
		err = runList()
	case "attach":
		err = withSession(os.Args[2:], runAttach)
	case "kill":
		err = withSession(os.Args[2:], func(c *client.Client, id string) error { return c.Kill(id) })
	case "rm":
		err = withSession(os.Args[2:], func(c *client.Client, id string) error { return c.Remove(id) })
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "windrunner:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  windrunner new [-name label] -- command args...
  windrunner ls
  windrunner attach <id-prefix>
  windrunner kill <id-prefix>
  windrunner rm <id-prefix>
  windrunner daemon`)
}

func socketPath() string {
	dir := os.Getenv("WINDRUNNER_DIR")
	if dir == "" {
		if stateHome := os.Getenv("XDG_STATE_HOME"); stateHome != "" {
			dir = filepath.Join(stateHome, "windrunner")
		} else if home, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(home, ".local", "state", "windrunner")
		} else {
			dir = filepath.Join(os.TempDir(), "windrunner")
		}
	}
	os.MkdirAll(dir, 0o700)
	return filepath.Join(dir, "daemon.sock")
}

func runDaemon() error {
	path := socketPath()
	os.Remove(path)
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	engine := windrunner.NewEngine()
	defer engine.Close()
	return server.Serve(engine, listener)
}

func connect() (*client.Client, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return client.EnsureDaemon(socketPath(), []string{self, "daemon"}, 5*time.Second)
}

// withSession resolves a session by unique ID prefix and hands it to fn.
func withSession(args []string, fn func(*client.Client, string) error) error {
	if len(args) != 1 {
		usage()
		os.Exit(2)
	}
	c, err := connect()
	if err != nil {
		return err
	}
	defer c.Close()
	sessions, err := c.List()
	if err != nil {
		return err
	}
	var matches []string
	for _, info := range sessions {
		if strings.HasPrefix(info.ID, args[0]) {
			matches = append(matches, info.ID)
		}
	}
	switch len(matches) {
	case 0:
		return fmt.Errorf("no session matches %q", args[0])
	case 1:
		return fn(c, matches[0])
	default:
		return fmt.Errorf("%q is ambiguous: %s", args[0], strings.Join(matches, ", "))
	}
}

func runList() error {
	c, err := connect()
	if err != nil {
		return err
	}
	defer c.Close()
	sessions, err := c.List()
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		fmt.Println("no sessions")
		return nil
	}
	for _, info := range sessions {
		state := "alive"
		if !info.Alive {
			state = fmt.Sprintf("exited(%d)", info.ExitCode)
		}
		label := info.Metadata["name"]
		if label == "" {
			label = info.Title
		}
		fmt.Printf("%s  %-10s %dx%d  %s\n", info.ID, state, info.Cols, info.Rows, label)
	}
	return nil
}

func runNew(args []string) error {
	name := ""
	if len(args) >= 2 && args[0] == "-name" {
		name = args[1]
		args = args[2:]
	}
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		shell := os.Getenv("SHELL")
		if shell == "" {
			shell = "/bin/sh"
		}
		args = []string{shell}
	}
	c, err := connect()
	if err != nil {
		return err
	}
	defer c.Close()

	cols, rows := 80, 24
	if width, height, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		cols, rows = width, height
	}
	metadata := map[string]string{}
	if name != "" {
		metadata["name"] = name
	}
	dir, _ := os.Getwd()
	info, err := c.Spawn(wire.Request{
		Command:  args[0],
		Args:     args[1:],
		Dir:      dir,
		Cols:     cols,
		Rows:     rows,
		Metadata: metadata,
	})
	if err != nil {
		return err
	}
	return runAttach(c, info.ID)
}

func runAttach(c *client.Client, id string) error {
	stdin := int(os.Stdin.Fd())
	if !term.IsTerminal(stdin) {
		return fmt.Errorf("attach needs a terminal")
	}
	// Match the session to this terminal before the snapshot is taken, so
	// the snapshot arrives pre-wrapped for the screen it is about to fill.
	if cols, rows, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		_ = c.Resize(id, cols, rows)
	}
	attachment, err := c.Attach(id, 256)
	if err != nil {
		return err
	}
	defer attachment.Close()

	previous, err := term.MakeRaw(stdin)
	if err != nil {
		return err
	}
	defer term.Restore(stdin, previous)

	snapshot := attachment.Snapshot()
	// Clear, replay the exact state, and from here every byte is live.
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
				if index := indexByte(chunk, detachKey); index >= 0 {
					if index > 0 {
						attachment.Write(chunk[:index])
					}
					return
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
			fmt.Fprintf(os.Stderr, "\r\n[detached: %s]\r\n", id)
			return nil
		case <-winch:
			if cols, rows, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
				_ = attachment.Resize(cols, rows)
			}
		case code := <-attachment.Exited():
			fmt.Fprintf(os.Stderr, "\r\n[session exited: %d]\r\n", code)
			return nil
		case chunk, ok := <-attachment.Output():
			if !ok {
				fmt.Fprintf(os.Stderr, "\r\n[connection lost]\r\n")
				return nil
			}
			os.Stdout.Write(chunk)
		}
	}
}

func indexByte(data []byte, target byte) int {
	for index, value := range data {
		if value == target {
			return index
		}
	}
	return -1
}
