package pglg

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

type backoff struct {
	initial    time.Duration
	max        time.Duration
	multiplier float64
	attempt    int
}

func (b *backoff) next() time.Duration {
	d := float64(b.initial) * math.Pow(b.multiplier, float64(b.attempt))
	if d > float64(b.max) {
		d = float64(b.max)
	}
	b.attempt++
	// Jitter: 75-100% of computed delay.
	jitter := 0.75 + rand.Float64()*0.25
	return time.Duration(d * jitter)
}

func (b *backoff) reset() {
	b.attempt = 0
}

func (b *backoff) wait(ctx context.Context) error {
	d := b.next()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
