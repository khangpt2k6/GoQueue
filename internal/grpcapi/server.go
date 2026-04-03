package grpcapi

import (
	"context"
	"io"
	"time"

	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/wal"
	goqueuev1 "github.com/2006t/goqueue/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	goqueuev1.UnimplementedBrokerServiceServer

	broker  *broker.Broker
	groups  *consumer.Manager
	metrics *metrics.Metrics
	wal     *wal.Log
}

func NewServer(b *broker.Broker, g *consumer.Manager, m *metrics.Metrics, l *wal.Log) *Server {
	return &Server{broker: b, groups: g, metrics: m, wal: l}
}

func (s *Server) Publish(ctx context.Context, req *goqueuev1.PublishRequest) (*goqueuev1.PublishResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	start := time.Now()
	if s.wal != nil {
		if err := s.wal.Append(req.Topic, req.Payload); err != nil {
			return nil, status.Errorf(codes.Internal, "wal append failed: %v", err)
		}
	}
	var (
		partition int
		offset    int64
		err       error
	)
	if req.Partition >= 0 {
		partition = int(req.Partition)
		offset, err = s.broker.PublishToPartition(req.Topic, partition, req.Payload)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid partition: %v", err)
		}
	} else {
		partition, offset, err = s.broker.PublishWithKey(req.Topic, req.Key, req.Payload)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
		}
	}
	if s.metrics != nil {
		s.metrics.PublishedTotal.Inc()
		s.metrics.ObservePublishLatency(start)
	}
	return &goqueuev1.PublishResponse{Offset: offset, Partition: int32(partition)}, nil
}

func (s *Server) Consume(req *goqueuev1.ConsumeRequest, stream grpc.ServerStreamingServer[goqueuev1.ConsumeMessage]) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}
	group := req.Group
	if group == "" {
		group = "default"
	}

	partition := int(req.Partition)
	var sub *broker.Subscription
	if partition < 0 {
		sub = s.broker.SubscribeAt(req.Topic, group, -1)
	} else {
		startOffset := int64(-1)
		if committed, ok := s.groups.GetPartition(req.Topic, group, partition); ok {
			startOffset = committed
		}
		sub = s.broker.SubscribePartitionAt(req.Topic, group, partition, startOffset)
	}
	defer s.broker.Unsubscribe(sub)

	for {
		msgs, err := sub.Next(stream.Context(), 128)
		if err != nil {
			if err == context.Canceled || err == io.EOF {
				return nil
			}
			return err
		}
		for _, msg := range msgs {
			out := &goqueuev1.ConsumeMessage{
				Offset:            msg.Offset,
				Payload:           msg.Payload,
				TimestampUnixNano: msg.Timestamp.UnixNano(),
				Partition:         int32(sub.Partition()),
			}
			if err := stream.Send(out); err != nil {
				return err
			}
		}
		latestOffset := msgs[len(msgs)-1].Offset + 1
		s.groups.CommitPartition(req.Topic, group, sub.Partition(), latestOffset)
		s.broker.AddConsumed(int64(len(msgs)))
		if s.metrics != nil {
			s.metrics.ConsumedTotal.Add(float64(len(msgs)))
			head, tail, err := s.broker.TopicPartitionInfo(req.Topic, sub.Partition())
			if err == nil {
				lag := tail - latestOffset
				if latestOffset < head {
					lag = tail - head
				}
				s.metrics.ConsumerLag.WithLabelValues(req.Topic, group).Set(float64(lag))
			}
		}
	}
}

func Register(grpcServer *grpc.Server, srv *Server) {
	goqueuev1.RegisterBrokerServiceServer(grpcServer, srv)
}
