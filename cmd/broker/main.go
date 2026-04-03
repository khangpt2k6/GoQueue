package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/2006t/goqueue/internal/broker"
	"github.com/2006t/goqueue/internal/consumer"
	"github.com/2006t/goqueue/internal/grpcapi"
	"github.com/2006t/goqueue/internal/metrics"
	"github.com/2006t/goqueue/internal/telemetry"
	"github.com/2006t/goqueue/internal/wal"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	tcpAddr := flag.String("tcp-addr", ":9090", "TCP broker listen address")
	grpcAddr := flag.String("grpc-addr", ":9095", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":2112", "Prometheus metrics listen address")
	walPath := flag.String("wal-path", "data/goqueue.wal", "WAL file path")
	nodeID := flag.String("node-id", "node-1", "node identifier for raft/dashboard labels")
	raftRole := flag.String("raft-role", "standalone", "raft role label: leader|follower|candidate|standalone")
	raftLeader := flag.String("raft-leader-id", "", "current raft leader id label")
	raftTerm := flag.Int64("raft-term", 1, "raft term value")
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
	leader := *raftLeader
	if strings.TrimSpace(leader) == "" {
		leader = *nodeID
	}
	m.SetRaftState(*nodeID, *raftRole, leader, *raftTerm)

	traceShutdown, err := telemetry.SetupTracing(context.Background(), "goqueue-broker")
	if err != nil {
		log.Fatalf("setup tracing: %v", err)
	}
	defer func() {
		_ = traceShutdown(context.Background())
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(reg))
		mux.HandleFunc("/raft/state", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node_id":   *nodeID,
				"role":      *raftRole,
				"leader_id": leader,
				"term":      *raftTerm,
			})
		})
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
		grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
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
