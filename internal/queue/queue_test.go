package queue

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestQueueLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	q, err := Open(path)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()

	id, err := q.Enqueue(Operation{
		Type:  OpPut,
		Key:   []byte("/eventual/a"),
		Value: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id == 0 {
		t.Fatalf("expected non-zero id")
	}

	op, err := q.Peek()
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if op.ID != id {
		t.Fatalf("expected id %d, got %d", id, op.ID)
	}

	if err := q.RecordFailure(id, "boom"); err != nil {
		t.Fatalf("record failure: %v", err)
	}

	op, err = q.Peek()
	if err != nil {
		t.Fatalf("peek after failure: %v", err)
	}
	if op.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", op.Attempts)
	}

	if err := q.MarkDone(id); err != nil {
		t.Fatalf("mark done: %v", err)
	}

	_, err = q.Peek()
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}
