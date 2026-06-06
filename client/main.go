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
	"os/exec"
	"time"

	pb "github.com/Uttam-Mahata/aeromq/go-controller/proto/aeromq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	controllerAddr := flag.String("controller", "127.0.0.1:8001", "Go controller gRPC address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]
	switch cmd {
	case "metadata":
		handleMetadata(*controllerAddr)
	case "create-topic":
		if len(args) < 4 {
			log.Fatalf("Usage: client create-topic <topic> <partitions> <replication_factor>")
		}
		var partitions, repFactor int
		_, err := fmt.Sscanf(args[2], "%d", &partitions)
		if err != nil {
			log.Fatalf("Invalid partitions: %v", err)
		}
		_, err = fmt.Sscanf(args[3], "%d", &repFactor)
		if err != nil {
			log.Fatalf("Invalid replication factor: %v", err)
		}
		handleCreateTopic(*controllerAddr, args[1], uint32(partitions), uint32(repFactor))
	case "produce":
		if len(args) < 4 {
			log.Fatalf("Usage: client produce <topic> <partition> <message>")
		}
		var partition int
		_, err := fmt.Sscanf(args[2], "%d", &partition)
		if err != nil {
			log.Fatalf("Invalid partition: %v", err)
		}
		handleProduce(*controllerAddr, args[1], uint32(partition), args[3])
	case "consume":
		if len(args) < 4 {
			log.Fatalf("Usage: client consume <topic> <partition> <offset> [--follow]")
		}
		var partition int
		var offset int64
		_, err := fmt.Sscanf(args[2], "%d", &partition)
		if err != nil {
			log.Fatalf("Invalid partition: %v", err)
		}
		_, err = fmt.Sscanf(args[3], "%d", &offset)
		if err != nil {
			log.Fatalf("Invalid offset: %v", err)
		}
		
		follow := false
		if len(args) > 4 && args[4] == "--follow" {
			follow = true
		}
		handleConsume(*controllerAddr, args[1], uint32(partition), uint64(offset), follow)
	case "integration-test":
		handleIntegrationTest(*controllerAddr)
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("AeroMQ CLI Client")
	fmt.Println("Usage:")
	fmt.Println("  client --controller <host:port> <command> [args]")
	fmt.Println("\nCommands:")
	fmt.Println("  metadata                                                   Fetch cluster topology details")
	fmt.Println("  create-topic <topic> <partitions> <replication_factor>      Create a new topic with configurations")
	fmt.Println("  produce <topic> <partition> <message>                      Publish a message payload to a partition")
	fmt.Println("  consume <topic> <partition> <offset> [--follow]            Retrieve messages starting from an offset")
	fmt.Println("  integration-test                                           Run comprehensive integration test suite")
}

func connectController(addr string) pb.DiscoveryServiceClient {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to controller gRPC: %v", err)
	}
	return pb.NewDiscoveryServiceClient(conn)
}

func handleMetadata(addr string) {
	client := connectController(addr)
	resp, err := client.GetMetadata(context.Background(), &pb.MetadataRequest{})
	if err != nil {
		log.Fatalf("Failed to fetch metadata: %v", err)
	}

	fmt.Println("--- Active Brokers ---")
	for _, b := range resp.Brokers {
		fmt.Printf("Broker ID: %d | Address: %s:%d\n", b.BrokerId, b.Host, b.Port)
	}

	fmt.Println("\n--- Topics & Partitions ---")
	for _, t := range resp.Topics {
		fmt.Printf("Topic: %s\n", t.Topic)
		for _, p := range t.Partitions {
			fmt.Printf("  └─ Partition %d | Leader: %d | Replicas: %v\n", p.PartitionId, p.LeaderId, p.ReplicaIds)
		}
	}
}

func handleCreateTopic(addr string, topic string, partitions uint32, repFactor uint32) {
	client := connectController(addr)
	resp, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:             topic,
		Partitions:        partitions,
		ReplicationFactor: repFactor,
	})
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	if resp.Success {
		fmt.Println(resp.Message)
	} else {
		fmt.Printf("Error creating topic: %s\n", resp.Message)
	}
}

func getPartitionLeaderAddr(addr string, topic string, partition uint32) (string, error) {
	client := connectController(addr)
	resp, err := client.GetMetadata(context.Background(), &pb.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return "", err
	}

	var leaderID uint32 = 999999
	found := false
	for _, t := range resp.Topics {
		if t.Topic == topic {
			for _, p := range t.Partitions {
				if p.PartitionId == partition {
					leaderID = p.LeaderId
					found = true
					break
				}
			}
		}
	}

	if !found {
		return "", fmt.Errorf("topic/partition not found")
	}

	for _, b := range resp.Brokers {
		if b.BrokerId == leaderID {
			return fmt.Sprintf("%s:%d", b.Host, b.Port), nil
		}
	}

	return "", fmt.Errorf("leader broker info not found in metadata")
}

func handleProduce(controllerAddr string, topic string, partition uint32, message string) {
	brokerAddr, err := getPartitionLeaderAddr(controllerAddr, topic, partition)
	if err != nil {
		log.Fatalf("Failed to find leader for partition: %v", err)
	}

	log.Printf("Connecting to broker leader at %s...", brokerAddr)
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to broker TCP data plane: %v", err)
	}
	defer conn.Close()

	offset, err := sendProduce(conn, topic, partition, message)
	if err != nil {
		log.Fatalf("Produce request failed: %v", err)
	}

	fmt.Printf("Message published successfully! Assigned Offset: %d\n", offset)
}

func sendProduce(conn net.Conn, topic string, partition uint32, message string) (uint64, error) {
	topicBytes := []byte(topic)
	msgBytes := []byte(message)
	bodyLen := 2 + len(topicBytes) + 4 + 4 + len(msgBytes)

	// Header: [magic (2)] [cmd (1)] [body_len (4)]
	header := make([]byte, 7)
	header[0] = 0xAE
	header[1] = 0x01
	header[2] = 1 // Cmd: Produce
	binary.BigEndian.PutUint32(header[3:7], uint32(bodyLen))

	// Body: [topic_len (2)] [topic] [partition (4)] [msg_len (4)] [message]
	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(topicBytes)))
	copy(body[2:2+len(topicBytes)], topicBytes)
	offset := 2 + len(topicBytes)
	binary.BigEndian.PutUint32(body[offset:offset+4], partition)
	binary.BigEndian.PutUint32(body[offset+4:offset+8], uint32(len(msgBytes)))
	copy(body[offset+8:], msgBytes)

	if _, err := conn.Write(header); err != nil {
		return 0, err
	}
	if _, err := conn.Write(body); err != nil {
		return 0, err
	}

	// Read Response: [magic (2)] [status (1)] [offset (8)]
	respHeader := make([]byte, 11)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		return 0, err
	}

	if respHeader[0] != 0xAE || respHeader[1] != 0x01 {
		return 0, fmt.Errorf("invalid response magic")
	}
	if respHeader[2] != 0 {
		return 0, fmt.Errorf("broker error status: %d", respHeader[2])
	}

	return binary.BigEndian.Uint64(respHeader[3:11]), nil
}

func handleConsume(controllerAddr string, topic string, partition uint32, startOffset uint64, follow bool) {
	brokerAddr, err := getPartitionLeaderAddr(controllerAddr, topic, partition)
	if err != nil {
		log.Fatalf("Failed to find leader for partition: %v", err)
	}

	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to broker TCP data plane: %v", err)
	}
	defer conn.Close()

	currentOffset := startOffset
	for {
		data, err := sendFetch(conn, topic, partition, currentOffset)
		if err != nil {
			log.Fatalf("Fetch request failed: %v", err)
		}

		if data != nil {
			fmt.Printf("[Offset %d]: %s\n", currentOffset, string(data))
			currentOffset++
			continue
		}

		if !follow {
			break
		}

		// Sleep before checking for new messages
		time.Sleep(500 * time.Millisecond)
	}
}

func sendFetch(conn net.Conn, topic string, partition uint32, startOffset uint64) ([]byte, error) {
	topicBytes := []byte(topic)
	bodyLen := 2 + len(topicBytes) + 4 + 8 + 4

	// Header: [magic (2)] [cmd (1)] [body_len (4)]
	header := make([]byte, 7)
	header[0] = 0xAE
	header[1] = 0x01
	header[2] = 2 // Cmd: Fetch
	binary.BigEndian.PutUint32(header[3:7], uint32(bodyLen))

	// Body: [topic_len (2)] [topic] [partition (4)] [offset (8)] [max_bytes (4)]
	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(topicBytes)))
	copy(body[2:2+len(topicBytes)], topicBytes)
	offset := 2 + len(topicBytes)
	binary.BigEndian.PutUint32(body[offset:offset+4], partition)
	binary.BigEndian.PutUint64(body[offset+4:offset+12], startOffset)
	binary.BigEndian.PutUint32(body[offset+12:offset+16], 65536) // maxBytes

	if _, err := conn.Write(header); err != nil {
		return nil, err
	}
	if _, err := conn.Write(body); err != nil {
		return nil, err
	}

	// Read response header: [magic (2)] [status (1)]
	respHeader := make([]byte, 3)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		return nil, err
	}

	if respHeader[0] != 0xAE || respHeader[1] != 0x01 {
		return nil, fmt.Errorf("invalid response magic")
	}

	status := respHeader[2]
	if status == 1 {
		return nil, nil // No data available
	}
	if status != 2 {
		return nil, fmt.Errorf("broker error status: %d", status)
	}

	// Read data size: [size (4)]
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf)

	payload := make([]byte, dataLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func handleIntegrationTest(controllerAddr string) {
	fmt.Println("==================================================")
	fmt.Println("   AeroMQ Data Plane Integration Test & Benchmark  ")
	fmt.Println("==================================================")

	topic := fmt.Sprintf("test-integration-%d", time.Now().Unix())
	partition := uint32(0)

	// Step 1: Create Topic with Replication Factor 2
	fmt.Printf("[1/7] Creating topic '%s' with 1 partition and replication factor 2...\n", topic)
	client := connectController(controllerAddr)
	createResp, err := client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:             topic,
		Partitions:        1,
		ReplicationFactor: 2,
	})
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	if !createResp.Success {
		log.Fatalf("Topic creation rejected: %s", createResp.Message)
	}
	fmt.Printf("Topic '%s' created: %s\n", topic, createResp.Message)

	// Wait for control plane to assign partitions and brokers to heartbeat/reconcile
	fmt.Println("Waiting 3 seconds for cluster reconciliation...")
	time.Sleep(3 * time.Second)

	// Step 2: Query metadata to identify leader and follower
	fmt.Println("[2/7] Fetching metadata to identify partition leader and follower...")
	meta, err := client.GetMetadata(context.Background(), &pb.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		log.Fatalf("Failed to fetch metadata: %v", err)
	}

	var leaderID, followerID uint32
	var leaderAddr, followerAddr string
	found := false
	for _, t := range meta.Topics {
		if t.Topic == topic {
			for _, p := range t.Partitions {
				if p.PartitionId == partition {
					leaderID = p.LeaderId
					for _, rID := range p.ReplicaIds {
						if rID != leaderID {
							followerID = rID
							break
						}
					}
					found = true
				}
			}
		}
	}
	if !found {
		log.Fatalf("Topic metadata not found")
	}

	for _, b := range meta.Brokers {
		if b.BrokerId == leaderID {
			leaderAddr = fmt.Sprintf("%s:%d", b.Host, b.Port)
		}
		if b.BrokerId == followerID {
			followerAddr = fmt.Sprintf("%s:%d", b.Host, b.Port)
		}
	}
	if leaderAddr == "" || followerAddr == "" {
		log.Fatalf("Could not resolve addresses for leader (%d) and follower (%d)", leaderID, followerID)
	}

	fmt.Printf("Partition 0 -> Leader Broker ID %d (%s) | Follower Broker ID %d (%s)\n", leaderID, leaderAddr, followerID, followerAddr)

	// Step 3: Write 6 messages to trigger segment rollover
	fmt.Println("[3/7] Producing 6 messages to trigger log segment rollover (limit is 5)...")
	leaderConn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}

	for i := 0; i < 6; i++ {
		msg := fmt.Sprintf("Message-%d-for-rollover-verification-test", i)
		offset, err := sendProduce(leaderConn, topic, partition, msg)
		if err != nil {
			log.Fatalf("Failed to produce message %d: %v", i, err)
		}
		fmt.Printf("  └─ Produced message %d, assigned offset: %d\n", i, offset)
	}
	leaderConn.Close()

	// Wait for follower replication and rollover
	fmt.Println("Waiting 3 seconds for follower replication and segment rollover...")
	time.Sleep(3 * time.Second)

	// Step 4: Verify Segment Rollover and cold storage directory layout
	fmt.Println("[4/7] Verifying segment rollover and cold storage layout on disk...")
	// We check locally under ./data/broker_<leaderID>/<topic>/partition_0/cold/
	coldLogPath := fmt.Sprintf("./data/cold_storage/broker_%d/%s/partition_%d/00000000000000000000.log", leaderID, topic, partition)
	coldIdxPath := fmt.Sprintf("./data/cold_storage/broker_%d/%s/partition_%d/00000000000000000000.idx", leaderID, topic, partition)
	activeLogPath := fmt.Sprintf("./data/broker_%d/%s/partition_%d/00000000000000000005.log", leaderID, topic, partition)

	if _, err := os.Stat(coldLogPath); os.IsNotExist(err) {
		log.Fatalf("Rolled over log file not found in cold storage at %s", coldLogPath)
	}
	if _, err := os.Stat(coldIdxPath); os.IsNotExist(err) {
		log.Fatalf("Rolled over idx file not found in cold storage at %s", coldIdxPath)
	}
	if _, err := os.Stat(activeLogPath); os.IsNotExist(err) {
		log.Fatalf("New active segment log file not found at %s", activeLogPath)
	}
	fmt.Println("[SUCCESS] Segment rollover and tiered storage layout on disk verified!")

	// Step 5: Verify Tiered Storage Retrieval
	fmt.Println("[5/7] Verifying tiered storage retrieval (fetching offset 0 from cold storage)...")
	leaderConn, err = net.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	defer leaderConn.Close()

	data, err := sendFetch(leaderConn, topic, partition, 0)
	if err != nil {
		log.Fatalf("Fetch offset 0 failed: %v", err)
	}
	if string(data) != "Message-0-for-rollover-verification-test" {
		log.Fatalf("Retrieved message mismatch: expected 'Message-0-for-rollover-verification-test', got '%s'", string(data))
	}
	fmt.Printf("[SUCCESS] Tiered storage retrieval verified! Retrieved offset 0: '%s'\n", string(data))

	// Step 6: Verify High-Watermark Blocking
	fmt.Println("[6/7] Verifying High-Watermark blocking...")
	fmt.Printf("Stopping follower Broker ID %d (%s) to halt replica synchronization...\n", followerID, followerAddr)
	
	// Terminate follower broker process
	killCmd := exec.Command("pkill", "-f", fmt.Sprintf("rust-broker --id %d", followerID))
	_ = killCmd.Run()
	time.Sleep(1 * time.Second) // wait for port to be released / socket to close

	// Produce a message (offset 6) to the leader.
	// Since replication factor = 2 and follower is offline, this message cannot be replicated.
	// High-Watermark will not advance.
	fmt.Println("Producing message to leader (assigned offset should be 6)...")
	offset6, err := sendProduce(leaderConn, topic, partition, "Message-6-HW-test")
	if err != nil {
		log.Fatalf("Failed to produce message at offset 6: %v", err)
	}
	fmt.Printf("Produced offset: %d. (Should be 6)\n", offset6)

	// Try to consume offset 6. Since HW hasn't advanced, this consumer fetch must block and return empty
	fmt.Println("Consuming offset 6 (expected to block/wait because HW has not advanced)...")
	startTime := time.Now()
	data6, err := sendFetch(leaderConn, topic, partition, 6)
	if err != nil {
		log.Fatalf("Fetch offset 6 failed: %v", err)
	}
	duration := time.Since(startTime)
	if data6 != nil {
		log.Fatalf("ERROR: Consumer fetched message before it was committed (replicated) to replica! Data: %s", string(data6))
	}
	fmt.Printf("Fetch request blocked for %v and returned empty (correct HW block behavior!)\n", duration)

	// Now start consumer in a background loop waiting for offset 6
	msgChan := make(chan string)
	go func() {
		for {
			conn, err := net.Dial("tcp", leaderAddr)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			data, err := sendFetch(conn, topic, partition, 6)
			conn.Close()
			if err == nil && data != nil {
				msgChan <- string(data)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Restart Follower broker
	fmt.Printf("Restarting Follower Broker ID %d...\n", followerID)
	followerPort := 9091
	if followerID == 2 {
		followerPort = 9092
	}
	
	logFile, err := os.OpenFile(fmt.Sprintf("broker%d.log", followerID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file for follower: %v", err)
	}
	defer logFile.Close()

	startCmd := exec.Command("./rust-broker/target/release/rust-broker",
		"--id", fmt.Sprintf("%d", followerID),
		"--host", "127.0.0.1",
		"--data-port", fmt.Sprintf("%d", followerPort),
		"--controller", "http://127.0.0.1:8001",
		"--storage-dir", fmt.Sprintf("./data/broker_%d", followerID),
	)
	startCmd.Stdout = logFile
	startCmd.Stderr = logFile
	err = startCmd.Start()
	if err != nil {
		log.Fatalf("Failed to restart follower broker: %v", err)
	}

	fmt.Println("Waiting for follower to sync and consumer to unblock...")
	select {
	case msg := <-msgChan:
		if msg == "Message-6-HW-test" {
			fmt.Println("[SUCCESS] Consumer unblocked and successfully read committed message!")
		} else {
			log.Fatalf("Consumer read incorrect message: %s", msg)
		}
	case <-time.After(15 * time.Second):
		log.Fatalf("TIMEOUT: Consumer did not receive unblocked message")
	}

	// Step 7: Verify ISR tracking with leader reelection
	fmt.Println("[7/7] Verifying ISR tracking and Leader reelection...")
	fmt.Printf("Stopping Leader Broker ID %d (%s)...\n", leaderID, leaderAddr)
	
	killLeaderCmd := exec.Command("pkill", "-f", fmt.Sprintf("rust-broker --id %d", leaderID))
	_ = killLeaderCmd.Run()

	fmt.Println("Waiting for Go Controller to detect leader failure and elect follower as the new leader (approx 10s)...")
	
	var newLeaderAddr string
	electionSuccess := false
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Second)
		meta, err = client.GetMetadata(context.Background(), &pb.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			continue
		}
		var currentLeaderID uint32
		for _, t := range meta.Topics {
			if t.Topic == topic {
				for _, p := range t.Partitions {
					if p.PartitionId == partition {
						currentLeaderID = p.LeaderId
					}
				}
			}
		}
		if currentLeaderID == followerID {
			// Found new leader is the follower!
			for _, b := range meta.Brokers {
				if b.BrokerId == currentLeaderID {
					newLeaderAddr = fmt.Sprintf("%s:%d", b.Host, b.Port)
					electionSuccess = true
					break
				}
			}
			break
		}
	}

	if !electionSuccess {
		log.Fatalf("Failover reelection failed. Follower was not promoted to leader")
	}

	fmt.Printf("New leader elected: Broker ID %d (%s)!\n", followerID, newLeaderAddr)

	// Produce a message to the new leader
	fmt.Println("Producing message to new leader...")
	newLeaderConn, err := net.Dial("tcp", newLeaderAddr)
	if err != nil {
		log.Fatalf("Failed to connect to new leader: %v", err)
	}
	defer newLeaderConn.Close()

	offset7, err := sendProduce(newLeaderConn, topic, partition, "Message-7-after-failover")
	if err != nil {
		log.Fatalf("Produce to new leader failed: %v", err)
	}
	fmt.Printf("Message published to new leader successfully! Offset: %d\n", offset7)

	// Consume message from the new leader
	data7, err := sendFetch(newLeaderConn, topic, partition, offset7)
	if err != nil {
		log.Fatalf("Fetch from new leader failed: %v", err)
	}
	if string(data7) != "Message-7-after-failover" {
		log.Fatalf("Data mismatch from new leader: expected 'Message-7-after-failover', got '%s'", string(data7))
	}
	fmt.Printf("Fetched offset %d from new leader successfully: '%s'\n", offset7, string(data7))
	fmt.Println("[SUCCESS] ISR tracking and Leader reelection verified!")

	fmt.Println("\n==================================================")
	fmt.Println("   ALL INTEGRATION TESTS PASSED SUCCESSFULLY!     ")
	fmt.Println("==================================================")
}
