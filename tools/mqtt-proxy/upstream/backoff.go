package upstream

import (
	"math/rand/v2"
	"time"
)

// Backoff doubles from Min to Max, with each delay scaled by a random factor
// in [1-Jitter, 1+Jitter) so many sessions do not reconnect in lockstep.
type Backoff struct {
	Min, Max time.Duration
	Jitter   float64
	Rand     func() float64 // [0,1); nil uses math/rand/v2
	cur      time.Duration
}

func (b *Backoff) Next() time.Duration {
	if b.cur == 0 {
		b.cur = b.Min
	} else {
		b.cur *= 2
	}
	if b.cur > b.Max {
		b.cur = b.Max
	}
	r := b.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(float64(b.cur) * (1 + b.Jitter*(2*r()-1)))
}

func (b *Backoff) Reset() { b.cur = 0 }
