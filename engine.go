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
	closed   bool
}

func NewEngine() *Engine {
	return &Engine{sessions: make(map[string]*Session)}
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

	s, err := startSession(id, spec)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		s.close()
		return nil, fmt.Errorf("windrunner: engine is closed")
	}
	e.sessions[id] = s
	e.mu.Unlock()
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
	}
}

// Close removes every session and refuses further spawns.
func (e *Engine) Close() {
	e.mu.Lock()
	e.closed = true
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessions = make(map[string]*Session)
	e.mu.Unlock()
	for _, s := range sessions {
		s.close()
	}
}

func newID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("windrunner: generate session id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
