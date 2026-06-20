package queue

import (
	"encoding/json"
	"fmt"
	"time"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("events")

type Queue struct {
	db *bolt.DB
}

type QueuedEvent struct {
	ID        string          `json:"id"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	Attempts  int             `json:"attempts"`
}

func New(path string) (*Queue, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open queue: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	fmt.Println("  disk queue ready:", path)
	return &Queue{db: db}, nil
}

func (q *Queue) Push(payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	event := QueuedEvent{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Payload:   data,
		CreatedAt: time.Now(),
		Attempts:  0,
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.Put([]byte(event.ID), eventData)
	})
}

func (q *Queue) Pop() (*QueuedEvent, error) {
	var event *QueuedEvent

	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()

		k, v := c.First()
		if k == nil {
			return nil
		}

		var e QueuedEvent
		if err := json.Unmarshal(v, &e); err != nil {
			return err
		}

		if err := b.Delete(k); err != nil {
			return err
		}

		event = &e
		return nil
	})

	return event, err
}


func (q *Queue) Len() int {
	count := 0
	q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		count = b.Stats().KeyN
		return nil
	})
	return count
}


func (q *Queue) Close() error {
	return q.db.Close()
}