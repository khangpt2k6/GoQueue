package broker

import (
	"context"
	"sync/atomic"
)

// Subscription tracks a single consumer's read position on a topic.
type Subscription struct {
	topic     *Topic
	group     string
	partition int
	offset    atomic.Int64
	notify    chan struct{}
	cancelFn  context.CancelFunc
}

func newSubscription(t *Topic, group string, partition int, startOffset int64) *Subscription {
	s := &Subscription{
		topic:     t,
		group:     group,
		partition: partition,
		notify:    make(chan struct{}, 1),
	}
	s.offset.Store(startOffset)
	return s
}

// Next blocks until a new message is available, then returns up to maxCount messages.
func (s *Subscription) Next(ctx context.Context, maxCount int) ([]Message, error) {
	for {
		msgs := s.topic.Fetch(s.offset.Load(), maxCount)
		if len(msgs) > 0 {
			s.offset.Add(int64(len(msgs)))
			return msgs, nil
		}
		// wait for notification or context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.notify:
		}
	}
}

// Commit moves the offset forward manually (for manual-ack mode).
func (s *Subscription) Commit(offset int64) {
	s.offset.Store(offset)
}

func (s *Subscription) Offset() int64  { return s.offset.Load() }
func (s *Subscription) Partition() int { return s.partition }
