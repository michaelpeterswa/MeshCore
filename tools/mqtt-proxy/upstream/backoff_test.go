package upstream

import (
	"testing"
	"time"
)

func TestBackoffDoublesToCap(t *testing.T) {
	b := &Backoff{Min: time.Second, Max: 5 * time.Minute, Jitter: 0.2, Rand: func() float64 { return 0.5 }}
	want := []time.Duration{1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300}
	for i, w := range want {
		if got := b.Next(); got != w*time.Second {
			t.Fatalf("step %d: %v want %v", i, got, w*time.Second)
		}
	}
	b.Reset()
	if got := b.Next(); got != time.Second {
		t.Fatalf("after reset %v", got)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	lo := &Backoff{Min: 10 * time.Second, Max: time.Minute, Jitter: 0.2, Rand: func() float64 { return 0 }}
	hi := &Backoff{Min: 10 * time.Second, Max: time.Minute, Jitter: 0.2, Rand: func() float64 { return 0.999999 }}
	if got := lo.Next(); got != 8*time.Second {
		t.Fatalf("low %v", got)
	}
	if got := hi.Next(); got < 11*time.Second || got > 12*time.Second {
		t.Fatalf("high %v", got)
	}
}

func TestBackoffDefaultRand(t *testing.T) {
	b := &Backoff{Min: time.Second, Max: time.Minute, Jitter: 0.2}
	if got := b.Next(); got < 800*time.Millisecond || got > 1200*time.Millisecond {
		t.Fatalf("%v", got)
	}
}
