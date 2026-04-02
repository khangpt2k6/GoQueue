package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/2006t/goqueue/internal/grpcapi"
	"github.com/2006t/goqueue/internal/protocol"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "publish":
		publishCmd(os.Args[2:])
	case "consume":
		consumeCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("goqueue publish --topic orders --addr localhost:9090 \"hello\"")
	fmt.Println("goqueue publish --grpc --addr localhost:9095 --topic orders \"hello\"")
	fmt.Println("goqueue consume --topic orders --group payment-service --addr localhost:9090")
	fmt.Println("goqueue consume --grpc --addr localhost:9095 --topic orders --group payment-service")
}

func publishCmd(args []string) {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "broker tcp address")
	topic := fs.String("topic", "", "topic name")
	useGRPC := fs.Bool("grpc", false, "use gRPC transport")
	_ = fs.Parse(args)

	if *topic == "" || fs.NArg() < 1 {
		log.Fatal("publish requires --topic and message payload")
	}
	msg := fs.Arg(0)
	if *useGRPC {
		publishGRPC(*addr, *topic, msg)
		return
	}
	publishTCP(*addr, *topic, msg)
}

func publishTCP(addr, topic, msg string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpPublish,
		Topic:   topic,
		Payload: []byte(msg),
	}); err != nil {
		log.Fatalf("send publish: %v", err)
	}
	resp, err := protocol.Decode(conn)
	if err != nil {
		log.Fatalf("read ack: %v", err)
	}
	if resp.Op == protocol.OpError {
		log.Fatalf("broker error: %s", string(resp.Payload))
	}
	if len(resp.Payload) == 8 {
		offset := int64(binary.BigEndian.Uint64(resp.Payload))
		fmt.Printf("published topic=%s offset=%d\n", topic, offset)
		return
	}
	fmt.Printf("published topic=%s\n", topic)
}

func publishGRPC(addr, topic, msg string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcapi.Codec())),
	)
	if err != nil {
		log.Fatalf("dial grpc broker: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &grpcapi.PublishRequest{Topic: topic, Payload: []byte(msg)}
	resp := new(grpcapi.PublishResponse)
	if err := conn.Invoke(ctx, grpcapi.PublishMethod, req, resp); err != nil {
		log.Fatalf("grpc publish failed: %v", err)
	}
	fmt.Printf("published topic=%s offset=%d\n", topic, resp.Offset)
}

func consumeCmd(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("addr", "localhost:9090", "broker tcp address")
	topic := fs.String("topic", "", "topic name")
	group := fs.String("group", "default", "consumer group")
	useGRPC := fs.Bool("grpc", false, "use gRPC transport")
	_ = fs.Parse(args)

	if *topic == "" {
		log.Fatal("consume requires --topic")
	}
	if *useGRPC {
		consumeGRPC(*addr, *topic, *group)
		return
	}
	consumeTCP(*addr, *topic, *group)
}

func consumeTCP(addr, topic, group string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()

	if err := protocol.Encode(conn, &protocol.Frame{
		Op:      protocol.OpSubscribe,
		Topic:   topic,
		Payload: []byte(group),
	}); err != nil {
		log.Fatalf("send subscribe: %v", err)
	}

	// Simple signal handling to make ctrl+c exit cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		_ = conn.Close()
		os.Exit(0)
	}()

	for {
		frame, err := protocol.Decode(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatalf("read message: %v", err)
		}
		if frame.Op == protocol.OpError {
			log.Fatalf("broker error: %s", string(frame.Payload))
		}
		if frame.Op != protocol.OpMessage {
			continue
		}
		fmt.Printf("[%s] %s\n", frame.Topic, string(frame.Payload))
	}
}

func consumeGRPC(addr, topic, group string) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcapi.Codec())),
	)
	if err != nil {
		log.Fatalf("dial grpc broker: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	desc := &grpc.StreamDesc{ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, grpcapi.ConsumeMethod)
	if err != nil {
		log.Fatalf("grpc consume stream failed: %v", err)
	}

	if err := stream.SendMsg(&grpcapi.ConsumeRequest{Topic: topic, Group: group}); err != nil {
		log.Fatalf("grpc send consume request failed: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("grpc close send failed: %v", err)
	}

	for {
		msg := new(grpcapi.ConsumeMessage)
		err := stream.RecvMsg(msg)
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			log.Fatalf("grpc read message failed: %v", err)
		}
		fmt.Printf("[%s] %s\n", topic, string(msg.Payload))
	}
}
