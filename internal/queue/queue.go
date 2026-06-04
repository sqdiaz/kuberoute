package queue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

var ErrEmpty = errors.New("queue empty")

var (
	opsBucket = []byte("ops")
)

type Type string

const (
	OpPut    Type = "put"
	OpDelete Type = "delete"
)

type Operation struct {
	ID        uint64    `json:"id"`
	Type      Type      `json:"type"`
	Key       []byte    `json:"key"`
	RangeEnd  []byte    `json:"range_end,omitempty"`
	Value     []byte    `json:"value,omitempty"`
	Lease     int64     `json:"lease,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
}

type Queue struct {
	db *bolt.DB
}

func Open(path string) (*Queue, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(opsBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Queue{db: db}, nil
}

func (q *Queue) Close() error {
	return q.db.Close()
}

func (q *Queue) Enqueue(op Operation) (uint64, error) {
	var id uint64
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(opsBucket)
		next, err := b.NextSequence()
		if err != nil {
			return err
		}
		id = next
		op.ID = id
		if op.CreatedAt.IsZero() {
			op.CreatedAt = time.Now().UTC()
		}
		payload, err := json.Marshal(op)
		if err != nil {
			return err
		}
		return b.Put(u64ToBytes(id), payload)
	})
	return id, err
}

func (q *Queue) Peek() (Operation, error) {
	var op Operation
	err := q.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(opsBucket).Cursor()
		_, v := c.First()
		if v == nil {
			return ErrEmpty
		}
		return json.Unmarshal(v, &op)
	})
	return op, err
}

func (q *Queue) MarkDone(id uint64) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(opsBucket).Delete(u64ToBytes(id))
	})
}

func (q *Queue) RecordFailure(id uint64, errMsg string) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(opsBucket)
		raw := b.Get(u64ToBytes(id))
		if raw == nil {
			return nil
		}

		var op Operation
		if err := json.Unmarshal(raw, &op); err != nil {
			return err
		}
		op.Attempts++
		op.LastError = errMsg

		updated, err := json.Marshal(op)
		if err != nil {
			return err
		}
		return b.Put(u64ToBytes(id), updated)
	})
}

func (q *Queue) Stats() (depth int, oldestAge time.Duration, err error) {
	now := time.Now().UTC()
	err = q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(opsBucket)
		depth = b.Stats().KeyN
		c := b.Cursor()
		_, first := c.First()
		if first == nil {
			oldestAge = 0
			return nil
		}
		var op Operation
		if err := json.Unmarshal(first, &op); err != nil {
			return err
		}
		oldestAge = now.Sub(op.CreatedAt)
		return nil
	})
	return depth, oldestAge, err
}

func u64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
