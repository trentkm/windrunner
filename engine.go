package windrunner

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
)

// Engine is a registry of live sessions. It can be embedded directly in a
// process, or served over a socket by the server package; the API is the
// same either way.
type Engine struct {
	mu       sync.Mutex
	sessions map[string]*Session
	subs     map[*EventSubscription]struct{}
	closed   bool
}

func NewEngine() *Engine {
	return &Engine{
		sessions: make(map[string]*Session),
		subs:     make(map[*EventSubscription]struct{}),
	}
}

// EventType names one kind of daemon-observed change.
type EventType string

const (
	EventSpawned EventType = "spawned"
	EventExited  EventType = "exited"
	EventRemoved EventType = "removed"
	// EventIdle and EventBusy report output activity: a session is busy
	// while its terminal is producing output and idle once it has been
	// quiet for its idle window. Sessions start busy; the first event is
	// always an idle.
	EventIdle EventType = "idle"
	EventBusy EventType = "busy"
)

// Event is one entry in the engine's pub/sub stream: a session appeared,
// went quiet, spoke again, exited, or was removed. Structured signals for
// coordination — richer than poke-and-peek, still not byte-splicing.
type Event struct {
	Type      EventType
	SessionID string
	Command   string // EventSpawned
	ExitCode  int    // EventExited
}

// Subscribe returns a feed of engine events from this moment on. A
// subscriber that stops reading is dropped, not waited for (see
// EventSubscription.Lagged). For any one session, its spawned event is
// published before every other event about it; beyond that, ordering
// across concurrent transitions is best-effort.
func (e *Engine) Subscribe(buffer int) *EventSubscription {
	if buffer <= 0 {
		buffer = 64
	}
	sub := &EventSubscription{engine: e, ch: make(chan Event, buffer)}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		sub.finish()
		return sub
	}
	e.subs[sub] = struct{}{}
	return sub
}

func (e *Engine) publish(event Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	for sub := range e.subs {
		sub.deliver(event, e)
	}
}

// Spawn starts a process on a PTY of its own and begins recording its
// terminal state.
func (e *Engine) Spawn(spec SpawnSpec) (*Session, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, fmt.Errorf("windrunner: engine is closed")
	}
	e.mu.Unlock()

	// The session's own events (idle/busy) are gated until the spawned
	// event is out, so no subscriber ever hears about a session before
	// hearing that it exists.
	ready := make(chan struct{})
	publish := func(event Event) {
		<-ready
		e.publish(event)
	}
	s, err := startSession(id, spec, publish)
	if err != nil {
		close(ready)
		return nil, err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		s.close()
		close(ready)
		return nil, fmt.Errorf("windrunner: engine is closed")
	}
	e.sessions[id] = s
	e.mu.Unlock()
	e.publish(Event{Type: EventSpawned, SessionID: id, Command: spec.Command})
	close(ready)
	go func() {
		<-s.Done()
		e.publish(Event{Type: EventExited, SessionID: id, ExitCode: s.ExitCode()})
	}()
	return s, nil
}

// Session finds a live session by ID.
func (e *Engine) Session(id string) (*Session, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sessions[id]
	return s, ok
}

// Sessions lists every session, oldest ID first for stable output.
func (e *Engine) Sessions() []*Session {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		list = append(list, s)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].id < list[j].id })
	return list
}

// Remove ends a session completely: process, PTY, terminal state, and
// subscribers. This is the only way session state is discarded — exit alone
// keeps it attachable.
func (e *Engine) Remove(id string) {
	e.mu.Lock()
	s, ok := e.sessions[id]
	delete(e.sessions, id)
	e.mu.Unlock()
	if ok {
		s.close()
		e.publish(Event{Type: EventRemoved, SessionID: id})
	}
}

// Close removes every session, ends every event subscription, and refuses
// further spawns.
func (e *Engine) Close() {
	e.mu.Lock()
	e.closed = true
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessions = make(map[string]*Session)
	subs := e.subs
	e.subs = make(map[*EventSubscription]struct{})
	e.mu.Unlock()
	for _, s := range sessions {
		s.close()
	}
	for sub := range subs {
		sub.finish()
	}
}

// EventSubscription is one reader of the engine's event stream.
type EventSubscription struct {
	engine *Engine
	ch     chan Event

	mu     sync.Mutex
	done   bool
	lagged bool
}

// Events yields engine events. The channel closes when the subscription is
// closed, the engine closes, or the reader falls too far behind.
func (sub *EventSubscription) Events() <-chan Event { return sub.ch }

// Lagged reports whether the subscription was dropped for reading too
// slowly. The recovery is to subscribe again and re-list sessions: fresh
// state is always cheaper than an unbounded backlog.
func (sub *EventSubscription) Lagged() bool {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.lagged
}

// Close ends the subscription. Safe to call at any time, any number of
// times.
func (sub *EventSubscription) Close() {
	sub.engine.mu.Lock()
	delete(sub.engine.subs, sub)
	sub.engine.mu.Unlock()
	sub.finish()
}

// deliver hands an event to the subscriber without ever blocking publish:
// a full buffer marks the subscriber lagged and drops it. Called with the
// engine lock held.
func (sub *EventSubscription) deliver(event Event, e *Engine) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.done {
		return
	}
	select {
	case sub.ch <- event:
	default:
		sub.lagged = true
		sub.done = true
		close(sub.ch)
		delete(e.subs, sub)
	}
}

// finish ends the stream normally.
func (sub *EventSubscription) finish() {
	sub.mu.Lock()
	if !sub.done {
		sub.done = true
		close(sub.ch)
	}
	sub.mu.Unlock()
}

func newID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("windrunner: generate session id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
