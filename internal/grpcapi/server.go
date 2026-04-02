package grpcapi

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

const (
	ServiceName   = "goqueue.v1.BrokerService"
	PublishMethod = "/" + ServiceName + "/Publish"
	ConsumeMethod = "/" + ServiceName + "/Consume"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                    { return "json" }

func Codec() encoding.Codec { return jsonCodec{} }

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
}

type PublishResponse struct {
	Offset int64 `json:"offset"`
}

type ConsumeRequest struct {
	Topic string `json:"topic"`
	Group string `json:"group"`
}

type ConsumeMessage struct {
	Offset            int64  `json:"offset"`
	Payload           []byte `json:"payload"`
	TimestampUnixNano int64  `json:"timestamp_unix_nano"`
}

type Server struct {
	broker  *broker.Broker
	groups  *consumer.Manager
	metrics *metrics.Metrics
	wal     *wal.Log
}

type brokerServiceServer interface {
	Publish(context.Context, *PublishRequest) (*PublishResponse, error)
	Consume(*ConsumeRequest, grpc.ServerStream) error
}

func NewServer(b *broker.Broker, g *consumer.Manager, m *metrics.Metrics, l *wal.Log) *Server {
	return &Server{broker: b, groups: g, metrics: m, wal: l}
}

func (s *Server) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	start := time.Now()
	if s.wal != nil {
		if err := s.wal.Append(req.Topic, req.Payload); err != nil {
			return nil, status.Errorf(codes.Internal, "wal append failed: %v", err)
		}
	}
	offset := s.broker.Publish(req.Topic, req.Payload)
	if s.metrics != nil {
		s.metrics.PublishedTotal.Inc()
		s.metrics.ObservePublishLatency(start)
	}
	return &PublishResponse{Offset: offset}, nil
}

func (s *Server) Consume(req *ConsumeRequest, stream grpc.ServerStream) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}
	group := req.Group
	if group == "" {
		group = "default"
	}

	startOffset := int64(-1)
	if committed, ok := s.groups.Get(req.Topic, group); ok {
		startOffset = committed
	}
	sub := s.broker.SubscribeAt(req.Topic, group, startOffset)
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
			out := &ConsumeMessage{
				Offset:            msg.Offset,
				Payload:           msg.Payload,
				TimestampUnixNano: msg.Timestamp.UnixNano(),
			}
			if err := stream.SendMsg(out); err != nil {
				return err
			}
		}
		latestOffset := msgs[len(msgs)-1].Offset + 1
		s.groups.Commit(req.Topic, group, latestOffset)
		s.broker.AddConsumed(int64(len(msgs)))
		if s.metrics != nil {
			s.metrics.ConsumedTotal.Add(float64(len(msgs)))
			head, tail, err := s.broker.TopicInfo(req.Topic)
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
	grpcServer.RegisterService(&grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*brokerServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Publish",
				Handler: func(service any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(PublishRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return service.(*Server).Publish(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     service,
						FullMethod: PublishMethod,
					}
					handler := func(ctx context.Context, req any) (any, error) {
						return service.(*Server).Publish(ctx, req.(*PublishRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Consume",
				ServerStreams: true,
				Handler: func(service any, stream grpc.ServerStream) error {
					in := new(ConsumeRequest)
					if err := stream.RecvMsg(in); err != nil {
						return err
					}
					return service.(*Server).Consume(in, stream)
				},
			},
		},
		Metadata: "proto/goqueue.proto",
	}, srv)
}
