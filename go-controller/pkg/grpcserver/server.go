package grpcserver

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/Uttam-Mahata/aeromq/go-controller/pkg/consensus"
	pb "github.com/Uttam-Mahata/aeromq/go-controller/proto/aeromq"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedControlServiceServer
	pb.UnimplementedDiscoveryServiceServer
	RaftNode *consensus.RaftNode
	mu       sync.Mutex
}

func NewServer(raftNode *consensus.RaftNode) *Server {
	s := &Server{
		RaftNode: raftNode,
	}
	
	// Start background task to clean up inactive brokers and trigger broker-failover
	go s.startFailureDetection()
	
	return s
}

func (s *Server) startFailureDetection() {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		if s.RaftNode.Raft.State() == raft.Leader {
			_ = s.RaftNode.Propose(consensus.CmdCleanInactive, nil)
		}
	}
}

func generateMemberID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("member-%x", b)
}

func (s *Server) checkLeader() error {
	if s.RaftNode.Raft.State() != raft.Leader {
		leaderAddr := s.RaftNode.Raft.Leader()
		return status.Errorf(codes.Unavailable, "node is not the cluster leader; current leader is: %s", leaderAddr)
	}
	return nil
}

// ControlService Implementation

func (s *Server) RegisterBroker(ctx context.Context, req *pb.RegisterBrokerRequest) (*pb.RegisterBrokerResponse, error) {
	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	payload := struct {
		ID   uint32 `json:"id"`
		Host string `json:"host"`
		Port int32  `json:"port"`
	}{
		ID:   req.BrokerId,
		Host: req.Host,
		Port: req.DataPort,
	}

	err := s.RaftNode.Propose(consensus.CmdRegisterBroker, payload)
	if err != nil {
		return &pb.RegisterBrokerResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.RegisterBrokerResponse{
		Success: true,
		Message: "Broker registered successfully",
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	type ReplicaOffsetPayload struct {
		Topic     string `json:"topic"`
		Partition uint32 `json:"partition"`
		Offset    int64  `json:"offset"`
	}

	payload := struct {
		ID             uint32                 `json:"id"`
		ReplicaOffsets []ReplicaOffsetPayload `json:"replica_offsets"`
	}{
		ID:             req.BrokerId,
		ReplicaOffsets: make([]ReplicaOffsetPayload, 0, len(req.ReplicaOffsets)),
	}

	for _, ro := range req.ReplicaOffsets {
		payload.ReplicaOffsets = append(payload.ReplicaOffsets, ReplicaOffsetPayload{
			Topic:     ro.Topic,
			Partition: ro.Partition,
			Offset:    ro.Offset,
		})
	}

	err := s.RaftNode.Propose(consensus.CmdBrokerHeartbeat, payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply heartbeat: %v", err)
	}

	// Read state to build the target assignments for this broker
	meta := s.RaftNode.FSM.GetMetadata(nil)
	
	assignedLeaders := []*pb.PartitionAssignment{}
	assignedFollowers := []*pb.PartitionAssignment{}

	for topicName, topicState := range meta.Topics {
		for pID, partition := range topicState.Partitions {
			if partition.LeaderID == req.BrokerId {
				assignedLeaders = append(assignedLeaders, &pb.PartitionAssignment{
					Topic:      topicName,
					Partition:  pID,
					LeaderId:   partition.LeaderID,
					ReplicaIds: partition.ReplicaIDs,
				})
			} else {
				// Check if broker is a replica/follower
				for _, repID := range partition.ReplicaIDs {
					if repID == req.BrokerId {
						assignedFollowers = append(assignedFollowers, &pb.PartitionAssignment{
							Topic:      topicName,
							Partition:  pID,
							LeaderId:   partition.LeaderID,
							ReplicaIds: partition.ReplicaIDs,
						})
						break
					}
				}
			}
		}
	}

	return &pb.HeartbeatResponse{
		Success:           true,
		AssignedLeaders:   assignedLeaders,
		AssignedFollowers: assignedFollowers,
	}, nil
}

// DiscoveryService Implementation

func (s *Server) GetMetadata(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	// Reads don't necessarily need to go through Raft consensus log (local read is fine if we accept eventual consistency),
	// but we should verify we are leader or read from FSM directly.
	meta := s.RaftNode.FSM.GetMetadata(req.Topics)

	brokers := []*pb.BrokerInfo{}
	for _, b := range meta.Brokers {
		if b.Active {
			brokers = append(brokers, &pb.BrokerInfo{
				BrokerId: b.ID,
				Host:     b.Host,
				Port:     b.Port,
			})
		}
	}

	topics := []*pb.TopicMetadata{}
	for topicName, tState := range meta.Topics {
		pMetaList := []*pb.PartitionMetadata{}
		for pID, partition := range tState.Partitions {
			replicaOffsetsProto := make(map[uint32]int64)
			for rID, off := range partition.ReplicaOffsets {
				replicaOffsetsProto[rID] = off
			}
			pMetaList = append(pMetaList, &pb.PartitionMetadata{
				PartitionId:    pID,
				LeaderId:       partition.LeaderID,
				ReplicaIds:     partition.ReplicaIDs,
				Isr:            partition.ISR,
				HighWatermark:  partition.HighWatermark,
				ReplicaOffsets: replicaOffsetsProto,
			})
		}
		topics = append(topics, &pb.TopicMetadata{
			Topic:      topicName,
			Partitions: pMetaList,
		})
	}

	return &pb.MetadataResponse{
		Brokers: brokers,
		Topics:  topics,
	}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	payload := struct {
		Name              string `json:"name"`
		Partitions        uint32 `json:"partitions"`
		ReplicationFactor uint32 `json:"replication_factor"`
	}{
		Name:              req.Topic,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
	}

	err := s.RaftNode.Propose(consensus.CmdCreateTopic, payload)
	if err != nil {
		return &pb.CreateTopicResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.CreateTopicResponse{
		Success: true,
		Message: fmt.Sprintf("Topic '%s' created successfully", req.Topic),
	}, nil
}

func (s *Server) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	memberID := req.MemberId
	if memberID == "" {
		memberID = generateMemberID()
	}

	payload := struct {
		GroupID  string   `json:"group_id"`
		MemberID string   `json:"member_id"`
		Topics   []string `json:"topics"`
	}{
		GroupID:  req.GroupId,
		MemberID: memberID,
		Topics:   req.Topics,
	}

	err := s.RaftNode.Propose(consensus.CmdJoinConsumerGroup, payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rebalance/join group failed: %v", err)
	}

	// Fetch updated state
	meta := s.RaftNode.FSM.GetMetadata(nil)
	cg, exists := meta.ConsumerGroups[req.GroupId]
	if !exists {
		return nil, status.Errorf(codes.Internal, "group not found after join proposal")
	}

	// Build assignment list
	assignments := []*pb.PartitionAssignment{}
	if pts, ok := cg.Assignments[memberID]; ok {
		for _, pt := range pts {
			// Find partition leader details
			leaderID := uint32(0)
			replicaIDs := []uint32{}
			if tState, ok := meta.Topics[pt.Topic]; ok {
				if pState, ok := tState.Partitions[pt.Partition]; ok {
					leaderID = pState.LeaderID
					replicaIDs = pState.ReplicaIDs
				}
			}
			assignments = append(assignments, &pb.PartitionAssignment{
				Topic:      pt.Topic,
				Partition:  pt.Partition,
				LeaderId:   leaderID,
				ReplicaIds: replicaIDs,
			})
		}
	}

	return &pb.JoinGroupResponse{
		MemberId:     memberID,
		GenerationId: cg.Generation,
		Assignments:  assignments,
	}, nil
}

func (s *Server) HeartbeatGroup(ctx context.Context, req *pb.HeartbeatGroupRequest) (*pb.HeartbeatGroupResponse, error) {
	// Basic validation, read local metadata
	meta := s.RaftNode.FSM.GetMetadata(nil)
	cg, exists := meta.ConsumerGroups[req.GroupId]
	if !exists {
		return &pb.HeartbeatGroupResponse{Status: pb.HeartbeatGroupResponse_UNKNOWN_MEMBER}, nil
	}

	member, exists := cg.Members[req.MemberId]
	if !exists {
		return &pb.HeartbeatGroupResponse{Status: pb.HeartbeatGroupResponse_UNKNOWN_MEMBER}, nil
	}

	// Update last seen in local memory (optional: could propose heartbeat to Raft, but group heartbeat
	// is high frequency so usually handled locally on coordinator, and only rebalances propagate.
	// We'll keep it simple: if generation is mismatched, trigger rebalance).
	member.LastSeen = time.Now()

	if cg.Generation != req.GenerationId {
		return &pb.HeartbeatGroupResponse{Status: pb.HeartbeatGroupResponse_REBALANCE_IN_PROGRESS}, nil
	}

	return &pb.HeartbeatGroupResponse{Status: pb.HeartbeatGroupResponse_OK}, nil
}

func (s *Server) CommitOffsets(ctx context.Context, req *pb.CommitOffsetsRequest) (*pb.CommitOffsetsResponse, error) {
	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	for _, offset := range req.Offsets {
		payload := struct {
			GroupID   string `json:"group_id"`
			Topic     string `json:"topic"`
			Partition uint32 `json:"partition"`
			Offset    int64  `json:"offset"`
		}{
			GroupID:   req.GroupId,
			Topic:     offset.Topic,
			Partition: offset.Partition,
			Offset:    offset.Offset,
		}

		err := s.RaftNode.Propose(consensus.CmdCommitOffset, payload)
		if err != nil {
			return &pb.CommitOffsetsResponse{Success: false}, nil
		}
	}

	return &pb.CommitOffsetsResponse{Success: true}, nil
}

func (s *Server) FetchOffsets(ctx context.Context, req *pb.FetchOffsetsRequest) (*pb.FetchOffsetsResponse, error) {
	offsets := []*pb.TopicPartitionOffset{}

	for _, topic := range req.Topics {
		// Fetch metadata for partitions
		meta := s.RaftNode.FSM.GetMetadata([]string{topic})
		tState, exists := meta.Topics[topic]
		if !exists {
			continue
		}

		for pID := range tState.Partitions {
			offsetVal := s.RaftNode.FSM.GetOffset(req.GroupId, topic, pID)
			if offsetVal >= 0 {
				if pState, exists := tState.Partitions[pID]; exists {
					if offsetVal > pState.HighWatermark {
						offsetVal = pState.HighWatermark
					}
				}
			}
			offsets = append(offsets, &pb.TopicPartitionOffset{
				Topic:     topic,
				Partition: pID,
				Offset:    offsetVal,
			})
		}
	}

	return &pb.FetchOffsetsResponse{Offsets: offsets}, nil
}
