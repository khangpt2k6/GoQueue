package broker

import (
	"context"
	"testing"
	"time"
)

func TestPublishWithKeyDeterministicPartition(t *testing.T) {
	b := New()
	b.EnsureTopic("orders", 8)

	p1, _, err := b.PublishWithKey("orders", "user-42", []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		p, _, err := b.PublishWithKey("orders", "user-42", []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		if p != p1 {
			t.Fatalf("partition drift: got %d want %d", p, p1)
		}
	}
}

func TestPublishWithKeyRoundRobin(t *testing.T) {
	b := New()
	b.EnsureTopic("events", 3)

	got := make(map[int]bool)
	for range 30 {
		p, _, err := b.PublishWithKey("events", "", []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		got[p] = true
	}
	if len(got) != 3 {
		t.Fatalf("expected all 3 partitions to receive messages, got %v", got)
	}
}

func TestSubscribeGroupAtUsesDeterministicPartition(t *testing.T) {
	b := New()
	b.EnsureTopic("logs", 6)

	s1 := b.SubscribeGroupAt("logs", "payment-service", -1)
	defer b.Unsubscribe(s1)
	s2 := b.SubscribeGroupAt("logs", "payment-service", -1)
	defer b.Unsubscribe(s2)
	if s1.Partition() != s2.Partition() {
		t.Fatalf("same group should map to same partition: %d vs %d", s1.Partition(), s2.Partition())
	}
}

func TestSubscribeSpecificPartition(t *testing.T) {
	b := New()
	b.EnsureTopic("metrics", 4)

	offset, err := b.PublishToPartition("metrics", 2, []byte("cpu=80"))
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	sub := b.SubscribePartitionAt("metrics", "g1", 2, 0)
	defer b.Unsubscribe(sub)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs, err := sub.Next(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "cpu=80" {
		t.Fatalf("unexpected messages: %+v", msgs)
	}
}
