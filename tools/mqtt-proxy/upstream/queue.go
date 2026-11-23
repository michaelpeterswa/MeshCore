package upstream

import (
	"context"
	"sync"
)

// Message is one publish bound for an upstream.
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool

	attempts int // failed publishes so far; see maxPublishAttempts
}

// Queue is a bounded FIFO that drops its oldest entry when full, so a long
// upstream outage costs old packets, not memory.
type Queue struct {
	mu    sync.Mutex
	items []Message
	cap   int
	drops uint64
	ready chan struct{} // closed and replaced whenever an item is added
}

func NewQueue(capacity int) *Queue {
	if capacity < 1 {
		capacity = 1
	}
	return &Queue{cap: capacity, ready: make(chan struct{})}
}

// Push appends m, dropping the oldest message if the queue is full.
func (q *Queue) Push(m Message) (dropped bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.cap {
		q.items = q.items[1:]
		q.drops++
		dropped = true
	}
	q.items = append(q.items, m)
	q.signalLocked()
	return dropped
}

// Unpop returns m to the front after a failed publish. If the queue filled
// up meanwhile, m is the oldest message and is the one dropped.
func (q *Queue) Unpop(m Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.cap {
		q.drops++
		return
	}
	q.items = append([]Message{m}, q.items...)
	q.signalLocked()
}

func (q *Queue) signalLocked() {
	close(q.ready)
	q.ready = make(chan struct{})
}

// Pop removes and returns the oldest message, blocking until one is
// available or ctx is done.
func (q *Queue) Pop(ctx context.Context) (Message, error) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			m := q.items[0]
			q.items[0] = Message{}
			q.items = q.items[1:]
			q.mu.Unlock()
			return m, nil
		}
		ready := q.ready
		q.mu.Unlock()
		select {
		case <-ready:
		case <-ctx.Done():
			return Message{}, ctx.Err()
		}
	}
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue) Drops() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.drops
}
