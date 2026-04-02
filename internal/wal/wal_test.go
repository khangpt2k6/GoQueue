package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	messages := []struct {
		topic   string
		payload string
	}{
		{"orders", "buy AAPL 100"},
		{"orders", "sell TSLA 50"},
		{"events", "user_signup"},
	}
	for _, m := range messages {
		if err := log.Append(m.topic, []byte(m.payload)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var recovered []Record
	err = Replay(path, func(r Record) error {
		recovered = append(recovered, r)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(recovered) != len(messages) {
		t.Fatalf("replayed %d records, want %d", len(recovered), len(messages))
	}
	for i, rec := range recovered {
		if rec.Topic != messages[i].topic {
			t.Errorf("[%d] topic: got %q, want %q", i, rec.Topic, messages[i].topic)
		}
		if string(rec.Payload) != messages[i].payload {
			t.Errorf("[%d] payload: got %q, want %q", i, string(rec.Payload), messages[i].payload)
		}
		if rec.Timestamp <= 0 {
			t.Errorf("[%d] timestamp should be positive, got %d", i, rec.Timestamp)
		}
	}
}

func TestReplayNonExistent(t *testing.T) {
	err := Replay("/tmp/goqueue-test-definitely-not-here.wal", func(r Record) error {
		t.Fatal("should not be called")
		return nil
	})
	if err != nil {
		t.Fatalf("replay nonexistent should return nil, got %v", err)
	}
}

func TestReplayTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = log.Append("t", []byte("full record"))
	_ = log.Close()

	// chop off a few bytes from the end to simulate crash mid-write
	data, _ := os.ReadFile(path)
	_ = os.WriteFile(path, data[:len(data)-3], 0o644)

	var count int
	err = Replay(path, func(r Record) error {
		count++
		return nil
	})
	// should not error — just stop at the truncated record
	if err != nil {
		t.Fatalf("replay truncated: %v", err)
	}
}

func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	log, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	payload := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = log.Append("bench-topic", payload)
	}
}
