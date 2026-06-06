package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/Uttam-Mahata/aeromq/go-controller/pkg/consensus"
	"github.com/Uttam-Mahata/aeromq/go-controller/pkg/grpcserver"
	pb "github.com/Uttam-Mahata/aeromq/go-controller/proto/aeromq"
	"google.golang.org/grpc"
)

func main() {
	nodeID := flag.String("id", "node1", "Unique node ID")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7001", "Raft communication address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:8001", "gRPC API service address")
	httpAddr := flag.String("http-addr", "127.0.0.1:9001", "HTTP control API address")
	dataDir := flag.String("data-dir", "", "Directory to store Raft snapshots (in-memory if empty)")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a new Raft cluster")
	joinAddr := flag.String("join", "", "Join address of an existing controller (e.g. http://127.0.0.1:9001)")
	flag.Parse()

	log.Printf("[AeroMQ Controller] Starting node %s...", *nodeID)

	// Initialize Raft Node
	raftNode, err := consensus.NewRaftNode(*nodeID, *raftAddr, *dataDir, *bootstrap)
	if err != nil {
		log.Fatalf("Failed to initialize Raft: %v", err)
	}

	// Set up gRPC Server
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("gRPC listen failed: %v", err)
	}
	grpcServer := grpc.NewServer()
	serverImpl := grpcserver.NewServer(raftNode)
	
	pb.RegisterControlServiceServer(grpcServer, serverImpl)
	pb.RegisterDiscoveryServiceServer(grpcServer, serverImpl)

	// Run gRPC Server in background
	go func() {
		log.Printf("[AeroMQ Controller] gRPC API server listening on %s", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// Handle Join HTTP API and stats
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		addr := r.URL.Query().Get("addr")
		if id == "" || addr == "" {
			http.Error(w, "missing id or addr parameters", http.StatusBadRequest)
			return
		}
		
		log.Printf("[AeroMQ Controller] Join request received from node %s (%s)", id, addr)
		if err := raftNode.Join(id, addr); err != nil {
			log.Printf("[AeroMQ Controller] Failed to join node %s: %v", id, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("joined successfully"))
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		meta := raftNode.FSM.GetMetadata(nil)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\n  \"node_id\": \"%s\",\n  \"state\": \"%s\",\n  \"leader\": \"%s\",\n", 
			*nodeID, raftNode.Raft.State().String(), raftNode.Raft.Leader())
		fmt.Fprintf(w, "  \"brokers_count\": %d,\n  \"topics_count\": %d\n}\n", 
			len(meta.Brokers), len(meta.Topics))
	})

	// Run HTTP server
	go func() {
		log.Printf("[AeroMQ Controller] HTTP API server listening on %s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// If join address is provided, try to join the cluster
	if *joinAddr != "" {
		go func() {
			// Small delay to ensure our own Raft transport is ready
			time.Sleep(1 * time.Second)
			joinUrl := fmt.Sprintf("%s/join?id=%s&addr=%s", *joinAddr, *nodeID, *raftAddr)
			log.Printf("[AeroMQ Controller] Attempting to join cluster via %s", joinUrl)
			
			client := http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(joinUrl)
			if err != nil {
				log.Printf("[AeroMQ Controller] Join attempt failed: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("[AeroMQ Controller] Join attempt returned non-OK status: %s", resp.Status)
				return
			}
			log.Printf("[AeroMQ Controller] Successfully joined the Raft cluster!")
		}()
	}

	// Block forever
	select {}
}
