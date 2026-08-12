# windrunner

A persistent terminal session engine for Go — the layer between spawning a
PTY and shipping a product.

Windrunner owns pseudo-terminals, runs an authoritative terminal emulator
for each one (pure Go, no cgo, no WASM), and keeps them alive in a daemon
that outlives any client. Attach from anywhere and get an exact snapshot —
screen, scrollback, cursor — followed by the live byte stream. Detach, come
back tomorrow, it's all still there.

It is the engine inside tools like tmux, screen, and every terminal-tab UI —
extracted, embeddable, and unopinionated about what you build on top.

## What it is

- **A library first.** `windrunner` is an importable engine: spawn sessions,
  write input, resize, subscribe to output, snapshot state. The daemon and
  wire protocol are packages, not requirements — embed the engine in-process
  if that's all you need.
- **Authoritative emulation.** Each session's terminal state lives in a real
  VT emulator ([`charmbracelet/x/vt`](https://github.com/charmbracelet/x)),
  fed directly from the PTY. Programs that query their terminal (cursor
  position, colors, capabilities) get real answers. Reattach snapshots are
  exact serializations of that state — not lossy screen scrapes, not replay
  logs.
- **Bytes on the wire.** Clients receive a snapshot, then raw output bytes,
  and send raw input bytes back. Bring any front end: a Go TUI, xterm.js in
  a browser, a recorder, a bot.
- **Sessions carry opaque metadata.** Tag sessions with whatever your
  product means by them — task, branch, agent, owner. Windrunner stores and
  returns the bag; it never interprets it.
- **Sessions can talk — deliberately.** `windrunner ls`, `peek <id>`
  (print a session's rendered screen), and `send <id> text...` (type text
  plus Enter) are a control plane any program with a shell can drive:
  discover peers, read their screens, prompt them. Speaking is opt-in per
  session (`new -peer`) — writing into a session's stdin is code execution
  as far as its process is concerned — and the daemon audit-logs every
  send, attributed via the `WINDRUNNER_SESSION` ID each child is spawned
  with. No stream-splicing: listening is snapshots, speaking is input.
  For coordination beyond poke-and-peek, `windrunner events` streams the
  daemon's pub/sub feed — session lifecycle plus idle/busy transitions —
  so a peer can wait for a session to go quiet instead of polling its
  screen.

## What it is not

- Not a terminal multiplexer for humans: no panes, no layouts, no copy mode,
  no keybindings, no status bar. Those belong to the products built on top.
- Not a renderer. Windrunner never draws; it maintains state and streams
  bytes.
- Not an orchestrator. It doesn't know what an "agent" is — or a "task", or
  a "workspace". Sessions are processes on PTYs with metadata you define.

## Status

Early 0.x. The API is settling against its first consumer
([stormlight](https://github.com/trentkm/stormlight)) and will break as it
does. Unix only for now (macOS and Linux).

## License

MIT
