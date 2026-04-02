package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

var ErrCorruptRecord = errors.New("wal: corrupt record")

// Record stores one append-only WAL entry.
type Record struct {
	Timestamp int64
	Topic     string
	Payload   []byte
}

// Log is a very small append-only WAL implementation.
type Log struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{
		f: f,
		w: bufio.NewWriterSize(f, 1<<20),
	}, nil
}

func (l *Log) Append(topic string, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	topicBytes := []byte(topic)
	if len(topicBytes) > 0xFFFF {
		return ErrCorruptRecord
	}

	header := make([]byte, 14)
	binary.BigEndian.PutUint64(header[0:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint16(header[8:10], uint16(len(topicBytes)))
	binary.BigEndian.PutUint32(header[10:14], uint32(len(payload)))

	if _, err := l.w.Write(header); err != nil {
		return err
	}
	if _, err := l.w.Write(topicBytes); err != nil {
		return err
	}
	if _, err := l.w.Write(payload); err != nil {
		return err
	}
	return l.w.Flush()
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil {
		_ = l.w.Flush()
	}
	if l.f != nil {
		return l.f.Close()
	}
	return nil
}

func Replay(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	header := make([]byte, 14)
	for {
		_, err := io.ReadFull(r, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		topicLen := int(binary.BigEndian.Uint16(header[8:10]))
		payloadLen := int(binary.BigEndian.Uint32(header[10:14]))
		if topicLen < 0 || payloadLen < 0 {
			return ErrCorruptRecord
		}

		body := make([]byte, topicLen+payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		rec := Record{
			Timestamp: int64(binary.BigEndian.Uint64(header[0:8])),
			Topic:     string(body[:topicLen]),
			Payload:   append([]byte(nil), body[topicLen:]...),
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}
