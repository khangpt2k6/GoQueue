package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"time"

	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/protocol"
	"github.com/2006t/goqueue/internal/wal"
)

type TCPServer struct {
	addr     string
	broker   *Broker
	wal      *wal.Log
	groups   *consumer.Manager
	metrics  *metrics.Metrics
	listener net.Listener
}

func NewTCPServer(addr string, b *Broker, l *wal.Log, g *consumer.Manager, m *metrics.Metrics) *TCPServer {
	return &TCPServer{
		addr:    addr,
		broker:  b,
		wal:     l,
		groups:  g,
		metrics: m,
	}
}

func (s *TCPServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("tcp broker listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *TCPServer) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		frame, err := protocol.Decode(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("decode frame error: %v", err)
			}
			return
		}
		if frame.Topic == "" {
			_ = protocol.Encode(conn, &protocol.Frame{Op: protocol.OpError, Topic: "error", Payload: []byte("topic required")})
			continue
		}

		switch frame.Op {
		case protocol.OpPublish:
			if err := s.handlePublish(conn, frame); err != nil {
				log.Printf("publish error: %v", err)
				return
			}
		case protocol.OpSubscribe:
			if err := s.handleSubscribe(conn, frame); err != nil && !errors.Is(err, io.EOF) {
				log.Printf("subscribe error: %v", err)
			}
			return
		default:
			_ = protocol.Encode(conn, &protocol.Frame{Op: protocol.OpError, Topic: frame.Topic, Payload: []byte("unsupported operation")})
		}
	}
}

func (s *TCPServer) handlePublish(conn net.Conn, frame *protocol.Frame) error {
	start := time.Now()
	if s.wal != nil {
		if err := s.wal.Append(frame.Topic, frame.Payload); err != nil {
			return err
		}
	}
	offset := s.broker.Publish(frame.Topic, frame.Payload)
	if s.metrics != nil {
		s.metrics.PublishedTotal.Inc()
		s.metrics.ObservePublishLatency(start)
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(offset))
	return protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpAck,
		Topic:   frame.Topic,
		Payload: out,
	})
}

func (s *TCPServer) handleSubscribe(conn net.Conn, frame *protocol.Frame) error {
	group := string(frame.Payload)
	if group == "" {
		group = "default"
	}

	startOffset := int64(-1)
	if committed, ok := s.groups.Get(frame.Topic, group); ok {
		startOffset = committed
	}
	sub := s.broker.SubscribeAt(frame.Topic, group, startOffset)
	defer s.broker.Unsubscribe(sub)

	ctx := context.Background()
	for {
		msgs, err := sub.Next(ctx, 128)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if err := protocol.Encode(conn, &protocol.Frame{
				Op:      protocol.OpMessage,
				Topic:   frame.Topic,
				Payload: msg.Payload,
			}); err != nil {
				return err
			}
		}

		latestOffset := msgs[len(msgs)-1].Offset + 1
		s.groups.Commit(frame.Topic, group, latestOffset)
		s.broker.AddConsumed(int64(len(msgs)))
		if s.metrics != nil {
			s.metrics.ConsumedTotal.Add(float64(len(msgs)))
			head, tail, err := s.broker.TopicInfo(frame.Topic)
			if err == nil {
				lag := tail - latestOffset
				if latestOffset < head {
					lag = tail - head
				}
				s.metrics.ConsumerLag.WithLabelValues(frame.Topic, group).Set(float64(lag))
			}
		}
	}
}
