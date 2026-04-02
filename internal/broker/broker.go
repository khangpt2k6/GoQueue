package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Broker manages all topics and routes messages between producers and consumers.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*Topic

	totalPublished atomic.Int64
	totalConsumed  atomic.Int64
}

func New() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}

// Publish writes a message to the named topic, creating it if needed.
func (b *Broker) Publish(topicName string, payload []byte) int64 {
	t := b.getOrCreate(topicName)
	offset := t.Publish(payload)
	b.totalPublished.Add(1)
	return offset
}

// Subscribe returns a Subscription starting at the latest offset.
func (b *Broker) Subscribe(topicName, group string) *Subscription {
	return b.SubscribeAt(topicName, group, -1)
}

// SubscribeAt returns a Subscription starting at a specific offset.
// If startOffset < 0, it starts from the current tail (latest).
func (b *Broker) SubscribeAt(topicName, group string, startOffset int64) *Subscription {
	t := b.getOrCreate(topicName)
	if startOffset < 0 {
		startOffset = t.Tail()
	}
	s := newSubscription(t, group, startOffset)
	t.addSub(s)
	return s
}

// Unsubscribe removes a subscription from its topic.
func (b *Broker) Unsubscribe(s *Subscription) {
	s.topic.removeSub(s)
}

// TopicInfo returns metadata for a topic.
func (b *Broker) TopicInfo(name string) (head, tail int64, err error) {
	b.mu.RLock()
	t, ok := b.topics[name]
	b.mu.RUnlock()
	if !ok {
		return 0, 0, fmt.Errorf("topic %q not found", name)
	}
	return t.Head(), t.Tail(), nil
}

// Topics returns a snapshot of all topic names.
func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for k := range b.topics {
		names = append(names, k)
	}
	return names
}

func (b *Broker) TotalPublished() int64 { return b.totalPublished.Load() }
func (b *Broker) TotalConsumed() int64  { return b.totalConsumed.Load() }
func (b *Broker) AddConsumed(n int64)   { b.totalConsumed.Add(n) }

func (b *Broker) getOrCreate(name string) *Topic {
	b.mu.RLock()
	t, ok := b.topics[name]
	b.mu.RUnlock()
	if ok {
		return t
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok = b.topics[name]; ok {
		return t
	}
	t = newTopic(name, defaultCapacity)
	b.topics[name] = t
	return t
}