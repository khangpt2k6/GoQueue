package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/grpcapi"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

func main() {
	tcpAddr := flag.String("tcp-addr", ":9090", "TCP broker listen address")
	grpcAddr := flag.String("grpc-addr", ":9095", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":2112", "Prometheus metrics listen address")
	walPath := flag.String("wal-path", "data/goqueue.wal", "WAL file path")
	flag.Parse()

	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	b := broker.New()
	if err := wal.Replay(*walPath, func(rec wal.Record) error {
		b.Publish(rec.Topic, rec.Payload)
		return nil
	}); err != nil {
		log.Fatalf("replay wal: %v", err)
	}

	logFile, err := wal.Open(*walPath)
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}
	defer logFile.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	groups := consumer.NewManager()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(reg))
		log.Printf("metrics listening on %s", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			log.Fatalf("metrics server stopped: %v", err)
		}
	}()

	go func() {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		grpcSrv := grpc.NewServer()
		grpcapi.Register(grpcSrv, grpcapi.NewServer(b, groups, m, logFile))
		log.Printf("grpc broker listening on %s", *grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("grpc server stopped: %v", err)
		}
	}()

	srv := broker.NewTCPServer(*tcpAddr, b, logFile, groups, m)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("tcp server stopped: %v", err)
	}
}
