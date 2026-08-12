// Command windrunner is the reference client and daemon: persistent
// terminal sessions you can create, list, and attach to — a minimal
// screen alternative that doubles as a demo of the library.
//
//	windrunner new [-name label] [-peer] -- command args...
//	windrunner ls
//	windrunner attach <id-prefix>
//	windrunner peek [-ansi] <id-prefix>
//	windrunner send <id-prefix> text...
//	windrunner kill <id-prefix>
//	windrunner rm <id-prefix>
//	windrunner daemon        (usually started for you)
//
// Detach from an attached session with ctrl+q.
//
// peek and send are the agent-facing control plane: peek prints a
// session's rendered screen and exits, send types text plus Enter into a
// session that opted in with -peer. A program that can run a shell can
// therefore discover its peers (ls), read their screens, and prompt them.
// Every send lands in the daemon's audit log, attributed to the sender's
// own session when it has one (WINDRUNNER_SESSION, stamped at spawn).
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/charmbracelet/x/vt"
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
	case "peek":
		err = runPeek(os.Args[2:])
	case "send":
		err = runSend(os.Args[2:])
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
  windrunner new [-name label] [-peer] -- command args...
  windrunner ls
  windrunner attach <id-prefix>
  windrunner peek [-ansi] <id-prefix>
  windrunner send <id-prefix> text...
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
	// The audit log is a guardrail, not a nicety: a daemon that cannot
	// record who sent what refuses to run rather than running silent.
	auditFile, err := os.OpenFile(filepath.Join(filepath.Dir(path), "audit.log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open audit log: %w", err)
	}
	defer auditFile.Close()
	os.Remove(path)
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	engine := windrunner.NewEngine()
	defer engine.Close()
	audit := log.New(auditFile, "", log.LstdFlags|log.Lmicroseconds)
	return server.Serve(engine, listener, server.WithAudit(audit))
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
		peer := "-"
		if info.Peer {
			peer = "peer"
		}
		label := info.Metadata["name"]
		if label == "" {
			label = info.Title
		}
		fmt.Printf("%s  %-10s %-4s  %dx%d  %s\n", info.ID, state, peer, info.Cols, info.Rows, label)
	}
	return nil
}

func runNew(args []string) error {
	name := ""
	peer := false
	for len(args) > 0 {
		if args[0] == "-name" && len(args) >= 2 {
			name = args[1]
			args = args[2:]
		} else if args[0] == "-peer" {
			peer = true
			args = args[1:]
		} else {
			break
		}
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
		Peer:     peer,
		Metadata: metadata,
	})
	if err != nil {
		return err
	}
	return runAttach(c, info.ID)
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[a-zA-Z]|\x1b\][^\x07]*\x07`)

// runPeek prints a session's screen and exits: listening without
// attaching. The default output is the visible screen as plain text — the
// shape an agent reads well — produced by replaying the snapshot into a
// fresh emulator. -ansi prints the exact snapshot bytes instead
// (scrollback, styling, cursor), for piping into a renderer.
func runPeek(args []string) error {
	ansi := false
	if len(args) > 0 && args[0] == "-ansi" {
		ansi = true
		args = args[1:]
	}
	return withSession(args, func(c *client.Client, id string) error {
		snapshot, err := c.Snapshot(id)
		if err != nil {
			return err
		}
		if ansi {
			_, err := os.Stdout.Write(snapshot.ANSI)
			return err
		}
		emu := vt.NewEmulator(snapshot.Cols, snapshot.Rows)
		emu.Write(snapshot.ANSI)
		lines := strings.Split(ansiPattern.ReplaceAllString(emu.Render(), ""), "\n")
		for i := range lines {
			lines[i] = strings.TrimRight(lines[i], " \t\r")
		}
		for len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
		fmt.Println(strings.Join(lines, "\n"))
		return nil
	})
}

// runSend types text plus Enter into a session — speaking without
// attaching. The target must have been spawned with -peer; the daemon
// audit-logs the send either way, attributed to this process's own
// session if it is running inside one.
func runSend(args []string) error {
	if len(args) < 2 {
		usage()
		os.Exit(2)
	}
	text := strings.Join(args[1:], " ") + "\r"
	return withSession(args[:1], func(c *client.Client, id string) error {
		return c.Send(id, []byte(text), os.Getenv("WINDRUNNER_SESSION"))
	})
}

func runAttach(c *client.Client, id string) error {
	result, err := c.Interactive(id, detachKey)
	if err != nil {
		return err
	}
	switch result {
	case client.Detached:
		fmt.Fprintf(os.Stderr, "\r\n[detached: %s]\r\n", id)
	case client.SessionExited:
		fmt.Fprintf(os.Stderr, "\r\n[session exited: %s]\r\n", id)
	case client.ConnectionLost:
		fmt.Fprintf(os.Stderr, "\r\n[connection lost]\r\n")
	}
	return nil
}
