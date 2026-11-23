package upstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func m(topic string) Message { return Message{Topic: topic} }

func TestQueueFIFO(t *testing.T) {
	q := NewQueue(4)
	q.Push(m("a"))
	q.Push(m("b"))
	ctx := context.Background()
	for _, want := range []string{"a", "b"} {
		got, err := q.Pop(ctx)
		if err != nil || got.Topic != want {
			t.Fatalf("got %q %v want %q", got.Topic, err, want)
		}
	}
}

func TestQueueDropsOldestWhenFull(t *testing.T) {
	q := NewQueue(3)
	drops := 0
	for _, s := range []string{"1", "2", "3", "4", "5"} {
		if q.Push(m(s)) {
			drops++
		}
	}
	if q.Len() != 3 || q.Drops() != 2 || drops != 2 {
		t.Fatalf("len %d drops %d reported %d", q.Len(), q.Drops(), drops)
	}
	got, _ := q.Pop(context.Background())
	if got.Topic != "3" {
		t.Fatalf("front %q", got.Topic)
	}
}

func TestQueuePopBlocksUntilPush(t *testing.T) {
	q := NewQueue(2)
	done := make(chan Message)
	go func() {
		msg, _ := q.Pop(context.Background())
		done <- msg
	}()
	select {
	case <-done:
		t.Fatal("Pop returned on empty queue")
	case <-time.After(50 * time.Millisecond):
	}
	q.Push(m("x"))
	select {
	case got := <-done:
		if got.Topic != "x" {
			t.Fatal(got.Topic)
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not wake")
	}
}

func TestQueuePopHonorsContext(t *testing.T) {
	q := NewQueue(2)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if _, err := q.Pop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestQueueUnpopGoesToFront(t *testing.T) {
	q := NewQueue(3)
	q.Push(m("b"))
	q.Unpop(m("a"))
	got, _ := q.Pop(context.Background())
	if got.Topic != "a" {
		t.Fatal(got.Topic)
	}
}

func TestQueueUnpopWhenFullDropsIt(t *testing.T) {
	q := NewQueue(1)
	q.Push(m("b"))
	q.Unpop(m("a"))
	if q.Len() != 1 || q.Drops() != 1 {
		t.Fatalf("len %d drops %d", q.Len(), q.Drops())
	}
	got, _ := q.Pop(context.Background())
	if got.Topic != "b" {
		t.Fatal(got.Topic)
	}
}
