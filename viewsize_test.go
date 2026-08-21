package windrunner

import (
	"testing"
)

// The terminal's size is a function of who is looking at it: each viewer
// may state a geometry, the newest statement wins, and a statement
// retires with its attachment. These tests pin that contract at the
// engine level; the server tests pin it over the wire.

func idle(t *testing.T, engine *Engine) *Session {
	t.Helper()
	return spawnShell(t, engine, `while :; do sleep 0.1; done`)
}

func wantSize(t *testing.T, s *Session, cols, rows int, story string) {
	t.Helper()
	waitFor(t, story, func() bool {
		c, r := s.Size()
		return c == cols && r == rows
	})
}

func TestAnAttachStatementSizesTheTerminalAndItsSnapshot(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	snapshot, sub := s.AttachWith(AttachOptions{Cols: 120, Rows: 40})
	defer sub.Close()
	if snapshot.Cols != 120 || snapshot.Rows != 40 {
		t.Fatalf("snapshot %dx%d, want 120x40: the statement must land "+
			"before the state that answers the attach",
			snapshot.Cols, snapshot.Rows)
	}
	if cols, rows := s.Size(); cols != 120 || rows != 40 {
		t.Fatalf("session %dx%d, want 120x40", cols, rows)
	}
}

func TestTheNewestStatementWins(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	_, first := s.AttachWith(AttachOptions{Cols: 100, Rows: 30})
	defer first.Close()
	_, second := s.AttachWith(AttachOptions{Cols: 90, Rows: 28})
	defer second.Close()
	wantSize(t, s, 90, 28, "the newer attach to take the terminal")

	// The first viewer speaks again and is newest again.
	if err := s.ResizeViewer(first, 110, 33); err != nil {
		t.Fatalf("ResizeViewer: %v", err)
	}
	wantSize(t, s, 110, 33, "the fresh statement to take the terminal")
}

func TestAStatementRetiresWithItsAttachment(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	_, resident := s.AttachWith(AttachOptions{Cols: 100, Rows: 30})
	defer resident.Close()
	_, visitor := s.AttachWith(AttachOptions{Cols: 80, Rows: 20})
	wantSize(t, s, 80, 20, "the visitor's statement to take the terminal")

	// The visitor hangs up; nothing else moves. The terminal must come
	// back to the viewer still watching — this is the whole model.
	visitor.Close()
	wantSize(t, s, 100, 30, "the terminal to settle on the resident")
}

func TestTheLastViewerLeavingFallsBackToBase(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine) // spawned at 80x24: that is the base

	_, only := s.AttachWith(AttachOptions{Cols: 132, Rows: 43})
	wantSize(t, s, 132, 43, "the statement to take the terminal")
	only.Close()
	wantSize(t, s, 80, 24, "the terminal to fall back to its base")
}

func TestTheBaseIsTheNewestWordButNeverRetires(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	_, viewer := s.AttachWith(AttachOptions{Cols: 100, Rows: 30})
	// A control-plane resize outranks the standing statement — it is
	// the newest word — and unlike a viewer's it survives every detach.
	if err := s.Resize(120, 40); err != nil {
		t.Fatalf("Resize: %v", err)
	}
	wantSize(t, s, 120, 40, "the base write to take the terminal")
	viewer.Close()
	wantSize(t, s, 120, 40, "the base to hold after the viewer leaves")
}

func TestAViewerWithoutALayoutMovesNothing(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	snapshot, sub := s.AttachWith(AttachOptions{})
	defer sub.Close()
	if snapshot.Cols != 80 || snapshot.Rows != 24 {
		t.Fatalf("snapshot %dx%d, want the untouched 80x24",
			snapshot.Cols, snapshot.Rows)
	}
	if cols, rows := s.Size(); cols != 80 || rows != 24 {
		t.Fatalf("session %dx%d, want the untouched 80x24", cols, rows)
	}
	// And closing it moves nothing either: it had no statement to retire.
	sub.Close()
	wantSize(t, s, 80, 24, "the terminal to stay where it was")
}

func TestALaggedViewersStatementRetires(t *testing.T) {
	engine := newTestEngine(t)
	s, release := spawnGated(t, engine, `for i in $(seq 1 200); do printf 'line %d\n' $i; done; while :; do sleep 0.1; done`)

	// A one-message buffer that is never drained: the flood drops it.
	_, laggard := s.AttachWith(AttachOptions{Buffer: 1, Cols: 70, Rows: 21})
	wantSize(t, s, 70, 21, "the laggard's statement to take the terminal")
	release()
	waitFor(t, "the laggard to be dropped", laggard.Lagged)
	// Dropped is detached: the statement retires the same way.
	wantSize(t, s, 80, 24, "the terminal to settle after the drop")
}

func TestAStatementBelowTwoByTwoIsRefused(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	_, sub := s.AttachWith(AttachOptions{Cols: 100, Rows: 30})
	defer sub.Close()
	if err := s.ResizeViewer(sub, 0, 0); err == nil {
		t.Fatal("a 0x0 statement was accepted")
	}
	if cols, rows := s.Size(); cols != 100 || rows != 30 {
		t.Fatalf("session %dx%d after a refused statement, want 100x30",
			cols, rows)
	}
}

func TestAStatementFromAnEndedAttachmentIsDropped(t *testing.T) {
	engine := newTestEngine(t)
	s := idle(t, engine)

	_, resident := s.AttachWith(AttachOptions{Cols: 100, Rows: 30})
	defer resident.Close()
	_, gone := s.AttachWith(AttachOptions{})
	gone.Close()
	if err := s.ResizeViewer(gone, 66, 22); err != nil {
		t.Fatalf("ResizeViewer on an ended attachment: %v", err)
	}
	wantSize(t, s, 100, 30, "the ghost's statement to move nothing")
}
