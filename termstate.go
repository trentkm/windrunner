package windrunner

import "fmt"

// termState shadows the render-critical terminal state the emulator holds
// but does not expose: scroll margins, the screen in use, and the handful
// of modes that change how bytes become cells. A snapshot that carries
// cells without this state seeds replicas that scroll and wrap
// differently from the session — drift that a partial repaint never
// heals. The tracker reads the same whole-sequence chunks the emulator
// does, so the two can not disagree about what was seen.
type termState struct {
	// top and bottom are DECSTBM margins, 1-based inclusive; zero means
	// the full screen.
	top, bottom int
	// lrEnabled is DECLRMM (?69): while set, CSI s is DECSLRM rather
	// than an ANSI.SYS cursor save.
	lrEnabled   bool
	left, right int
	origin      bool // DECOM ?6
	autowrapOff bool // DECAWM ?7 reset
	cursorHide  bool // DECTCEM ?25 reset
	alt         bool // alternate screen (47/1047/1049)
}

// observe scans one whole-sequence chunk (see completeBoundary) for the
// state changes worth shadowing.
func (t *termState) observe(chunk []byte) {
	for index := 0; index < len(chunk); index++ {
		if chunk[index] != 0x1b {
			continue
		}
		if index+1 >= len(chunk) {
			return
		}
		switch chunk[index+1] {
		case 'c': // RIS: full reset
			*t = termState{}
			index++
		case '[':
			consumed := t.observeCSI(chunk[index+2:])
			index += 1 + consumed
		}
	}
}

// observeCSI reads one CSI sequence starting after the bracket and
// returns how many bytes it consumed.
func (t *termState) observeCSI(rest []byte) int {
	private := false
	params := []int{}
	current, hasCurrent := 0, false
	index := 0
	var intermediates []byte
	for ; index < len(rest); index++ {
		b := rest[index]
		switch {
		case b == '?' && index == 0:
			private = true
		case b >= '0' && b <= '9':
			current = current*10 + int(b-'0')
			hasCurrent = true
		case b == ';':
			params = append(params, current)
			current, hasCurrent = 0, false
		case b >= 0x20 && b <= 0x2f:
			intermediates = append(intermediates, b)
		case b >= 0x40 && b <= 0x7e:
			if hasCurrent {
				params = append(params, current)
			}
			t.apply(private, intermediates, params, b)
			return index + 1
		default:
			return index + 1
		}
	}
	return index
}

func (t *termState) apply(private bool, intermediates []byte, params []int, final byte) {
	if len(intermediates) == 1 && intermediates[0] == '!' && final == 'p' {
		*t = termState{} // DECSTR soft reset
		return
	}
	if len(intermediates) > 0 {
		return
	}
	switch {
	case !private && final == 'r':
		t.top, t.bottom = 0, 0
		if len(params) >= 2 && params[0] >= 1 && params[1] > params[0] {
			t.top, t.bottom = params[0], params[1]
		}
	case !private && final == 's' && t.lrEnabled:
		t.left, t.right = 0, 0
		if len(params) >= 2 && params[0] >= 1 && params[1] > params[0] {
			t.left, t.right = params[0], params[1]
		}
	case private && (final == 'h' || final == 'l'):
		set := final == 'h'
		for _, mode := range params {
			switch mode {
			case 6:
				t.origin = set
			case 7:
				t.autowrapOff = !set
			case 25:
				t.cursorHide = !set
			case 47, 1047, 1049:
				t.alt = set
			case 69:
				t.lrEnabled = set
				if !set {
					t.left, t.right = 0, 0
				}
			}
		}
	}
}

// resize clamps margins the way a terminal does: a resize resets them.
func (t *termState) resize() {
	t.top, t.bottom = 0, 0
	t.left, t.right = 0, 0
}

// replay serializes the tracked state as the sequences that reestablish
// it. The alternate-screen switch is not included: it must precede the
// cell replay, so the snapshot emits it separately.
func (t *termState) replay() string {
	var out []byte
	flag := func(mode int, set bool) {
		final := byte('l')
		if set {
			final = 'h'
		}
		out = append(out, fmt.Sprintf("\x1b[?%d%c", mode, final)...)
	}
	flag(7, !t.autowrapOff)
	flag(25, !t.cursorHide)
	if t.lrEnabled {
		flag(69, true)
		if t.left > 0 {
			out = append(out, fmt.Sprintf("\x1b[%d;%ds", t.left, t.right)...)
		}
	}
	if t.top > 0 {
		out = append(out, fmt.Sprintf("\x1b[%d;%dr", t.top, t.bottom)...)
	}
	// Origin mode last: with margins in place it homes the cursor to the
	// region, and the caller's absolute CUP that follows must override it
	// deliberately — so only assert it when set, and callers park the
	// cursor with origin-relative coordinates in that case.
	if t.origin {
		flag(6, true)
	}
	return string(out)
}
