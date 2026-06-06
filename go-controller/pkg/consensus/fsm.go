package consensus

import (
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// PartitionTopic represents a topic-partition pair
type PartitionTopic struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
}

// BrokerStatus tracks active brokers
type BrokerStatus struct {
	ID       uint32    `json:"id"`
	Host     string    `json:"host"`
	Port     int32     `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	Active   bool      `json:"active"`
}

// PartitionState tracks partition leader and replicas
type PartitionState struct {
	PartitionID    uint32            `json:"partition_id"`
	LeaderID       uint32            `json:"leader_id"`
	ReplicaIDs     []uint32          `json:"replica_ids"`
	ISR            []uint32          `json:"isr"`
	HighWatermark  int64             `json:"high_watermark"`
	ReplicaOffsets map[uint32]int64  `json:"replica_offsets"` // key: broker_id -> offset
}

// TopicState tracks topic metadata
type TopicState struct {
	Name       string                     `json:"name"`
	Partitions map[uint32]*PartitionState `json:"partitions"`
}

// GroupMember tracks a consumer in a consumer group
type GroupMember struct {
	ID       string    `json:"id"`
	Topics   []string  `json:"topics"`
	LastSeen time.Time `json:"last_seen"`
}

// ConsumerGroupState tracks consumer group state
type ConsumerGroupState struct {
	GroupID     string                      `json:"group_id"`
	Generation  uint32                      `json:"generation"`
	Members     map[string]*GroupMember     `json:"members"`
	Assignments map[string][]PartitionTopic `json:"assignments"` // key: member_id
}

// ClusterState is the state replicated by Raft
type ClusterState struct {
	Brokers        map[uint32]*BrokerStatus        `json:"brokers"`
	Topics         map[string]*TopicState          `json:"topics"`
	ConsumerGroups map[string]*ConsumerGroupState  `json:"consumer_groups"`
	Offsets        map[string]int64                `json:"offsets"` // key: "group_id:topic:partition"
}

// FSM implements raft.FSM
type FSM struct {
	mu    sync.RWMutex
	state ClusterState
}

func NewFSM() *FSM {
	return &FSM{
		state: ClusterState{
			Brokers:        make(map[uint32]*BrokerStatus),
			Topics:         make(map[string]*TopicState),
			ConsumerGroups: make(map[string]*ConsumerGroupState),
			Offsets:        make(map[string]int64),
		},
	}
}

// Command types
const (
	CmdRegisterBroker     = "register_broker"
	CmdBrokerHeartbeat    = "broker_heartbeat"
	CmdCreateTopic        = "create_topic"
	CmdAssignPartitions   = "assign_partitions"
	CmdJoinConsumerGroup  = "join_consumer_group"
	CmdCommitOffset       = "commit_offset"
	CmdCleanInactive      = "clean_inactive"
)

type Command struct {
	Op      string          `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {
	case CmdRegisterBroker:
		var payload struct {
			ID   uint32 `json:"id"`
			Host string `json:"host"`
			Port int32  `json:"port"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			f.state.Brokers[payload.ID] = &BrokerStatus{
				ID:       payload.ID,
				Host:     payload.Host,
				Port:     payload.Port,
				LastSeen: time.Now(),
				Active:   true,
			}
		}

	case CmdBrokerHeartbeat:
		var payload struct {
			ID             uint32 `json:"id"`
			ReplicaOffsets []struct {
				Topic     string `json:"topic"`
				Partition uint32 `json:"partition"`
				Offset    int64  `json:"offset"`
			} `json:"replica_offsets"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if broker, exists := f.state.Brokers[payload.ID]; exists {
				broker.LastSeen = time.Now()
				broker.Active = true
			}
			// Update replica offsets
			for _, ro := range payload.ReplicaOffsets {
				if topicState, exists := f.state.Topics[ro.Topic]; exists {
					if partitionState, exists := topicState.Partitions[ro.Partition]; exists {
						if partitionState.ReplicaOffsets == nil {
							partitionState.ReplicaOffsets = make(map[uint32]int64)
						}
						partitionState.ReplicaOffsets[payload.ID] = ro.Offset
					}
				}
			}
			f.updateISRAndHW()
		}

	case CmdCreateTopic:
		var payload struct {
			Name              string `json:"name"`
			Partitions        uint32 `json:"partitions"`
			ReplicationFactor uint32 `json:"replication_factor"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if _, exists := f.state.Topics[payload.Name]; !exists {
				topic := &TopicState{
					Name:       payload.Name,
					Partitions: make(map[uint32]*PartitionState),
				}
				
				// Pick active brokers to distribute partitions
				activeBrokers := f.getActiveBrokerIDs()
				
				for i := uint32(0); i < payload.Partitions; i++ {
					leaderID := uint32(0)
					replicas := []uint32{}
					
					if len(activeBrokers) > 0 {
						// Simple round robin selection
						leaderID = activeBrokers[int(i)%len(activeBrokers)]
						
						// Pick replication factor replicas
						for r := uint32(0); r < payload.ReplicationFactor && r < uint32(len(activeBrokers)); r++ {
							replicaIdx := (int(i) + int(r)) % len(activeBrokers)
							replicas = append(replicas, activeBrokers[replicaIdx])
						}
					}
					
					topic.Partitions[i] = &PartitionState{
						PartitionID:    i,
						LeaderID:       leaderID,
						ReplicaIDs:     replicas,
						ISR:            append([]uint32(nil), replicas...),
						HighWatermark:  0,
						ReplicaOffsets: make(map[uint32]int64),
					}
					for _, rID := range replicas {
						topic.Partitions[i].ReplicaOffsets[rID] = 0
					}
				}
				f.state.Topics[payload.Name] = topic
				f.updateISRAndHW()
			}
		}

	case CmdAssignPartitions:
		var payload struct {
			Topic      string                     `json:"topic"`
			Partitions map[uint32]*PartitionState `json:"partitions"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if t, exists := f.state.Topics[payload.Topic]; exists {
				for pID, pState := range payload.Partitions {
					t.Partitions[pID] = pState
					if t.Partitions[pID].ReplicaOffsets == nil {
						t.Partitions[pID].ReplicaOffsets = make(map[uint32]int64)
					}
				}
				f.updateISRAndHW()
			}
		}

	case CmdJoinConsumerGroup:
		var payload struct {
			GroupID  string   `json:"group_id"`
			MemberID string   `json:"member_id"`
			Topics   []string `json:"topics"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			g, exists := f.state.ConsumerGroups[payload.GroupID]
			if !exists {
				g = &ConsumerGroupState{
					GroupID:     payload.GroupID,
					Generation:  0,
					Members:     make(map[string]*GroupMember),
					Assignments: make(map[string][]PartitionTopic),
				}
				f.state.ConsumerGroups[payload.GroupID] = g
			}
			
			g.Members[payload.MemberID] = &GroupMember{
				ID:       payload.MemberID,
				Topics:   payload.Topics,
				LastSeen: time.Now(),
			}
			
			// Trigger a rebalance!
			f.rebalanceGroup(g)
		}

	case CmdCommitOffset:
		var payload struct {
			GroupID   string `json:"group_id"`
			Topic     string `json:"topic"`
			Partition uint32 `json:"partition"`
			Offset    int64  `json:"offset"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			key := payload.GroupID + ":" + payload.Topic + ":" + string(rune(payload.Partition)) // better key formatting
			// Wait, let's use a cleaner key format:
			key = payload.GroupID + "/" + payload.Topic + "/" + string(rune(payload.Partition))
			f.state.Offsets[key] = payload.Offset
		}

	case CmdCleanInactive:
		// Clean inactive brokers and trigger failover if they were partition leaders
		now := time.Now()
		for _, broker := range f.state.Brokers {
			if broker.Active && now.Sub(broker.LastSeen) > 8*time.Second {
				broker.Active = false
				f.handleBrokerFailure(broker.ID)
			}
		}
		f.updateISRAndHW()
	}

	return nil
}

func (f *FSM) updateISRAndHW() {
	now := time.Now()
	for _, topic := range f.state.Topics {
		for _, partition := range topic.Partitions {
			leaderID := partition.LeaderID
			
			if partition.ReplicaOffsets == nil {
				partition.ReplicaOffsets = make(map[uint32]int64)
			}
			
			leaderOffset := int64(0)
			if val, ok := partition.ReplicaOffsets[leaderID]; ok {
				leaderOffset = val
			}
			
			newISR := []uint32{}
			leaderBroker, leaderExists := f.state.Brokers[leaderID]
			if leaderExists && leaderBroker.Active && now.Sub(leaderBroker.LastSeen) <= 8*time.Second {
				newISR = append(newISR, leaderID)
			}
			
			for _, rID := range partition.ReplicaIDs {
				if rID == leaderID {
					continue
				}
				broker, exists := f.state.Brokers[rID]
				if !exists || !broker.Active || now.Sub(broker.LastSeen) > 8*time.Second {
					continue
				}
				offset := partition.ReplicaOffsets[rID]
				lag := leaderOffset - offset
				if lag <= 10 {
					newISR = append(newISR, rID)
				}
			}
			
			partition.ISR = newISR
			
			if len(newISR) > 0 {
				var minOffset int64
				for i, rID := range newISR {
					off := partition.ReplicaOffsets[rID]
					if i == 0 || off < minOffset {
						minOffset = off
					}
				}
				partition.HighWatermark = minOffset
			} else {
				partition.HighWatermark = 0
			}
		}
	}
}

func (f *FSM) getActiveBrokerIDs() []uint32 {
	ids := []uint32{}
	for id, b := range f.state.Brokers {
		if b.Active {
			ids = append(ids, id)
		}
	}
	return ids
}

func (f *FSM) handleBrokerFailure(failedID uint32) {
	// Find all partitions where this broker was leader, and assign a new leader
	activeBrokers := f.getActiveBrokerIDs()
	if len(activeBrokers) == 0 {
		return // No active brokers to failover to!
	}

	for _, topic := range f.state.Topics {
		for _, partition := range topic.Partitions {
			if partition.LeaderID == failedID {
				// Pick a new leader from replicas if possible, else round robin active
				newLeader := uint32(0)
				for _, repID := range partition.ReplicaIDs {
					if repID != failedID {
						if b, exists := f.state.Brokers[repID]; exists && b.Active {
							newLeader = repID
							break
						}
					}
				}
				if newLeader == 0 {
					newLeader = activeBrokers[0] // Fallback
				}
				partition.LeaderID = newLeader
			}
		}
	}
}

func (f *FSM) rebalanceGroup(g *ConsumerGroupState) {
	g.Generation++
	g.Assignments = make(map[string][]PartitionTopic)

	// Collect all topics that members are interested in
	topicToMembers := make(map[string][]string)
	for memberID, member := range g.Members {
		for _, topic := range member.Topics {
			topicToMembers[topic] = append(topicToMembers[topic], memberID)
		}
	}

	// For each topic subscribed, distribute its partitions among subscribed members
	for topicName, memberIDs := range topicToMembers {
		if len(memberIDs) == 0 {
			continue
		}
		topic, exists := f.state.Topics[topicName]
		if !exists {
			continue
		}

		for pID := range topic.Partitions {
			// Round robin assignment
			assignedMember := memberIDs[int(pID)%len(memberIDs)]
			g.Assignments[assignedMember] = append(g.Assignments[assignedMember], PartitionTopic{
				Topic:     topicName,
				Partition: pID,
			})
		}
	}
}

// GetMetadata returns current cluster snapshot metadata
func (f *FSM) GetMetadata(topics []string) ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Perform a deep copy of current state
	res := ClusterState{
		Brokers:        make(map[uint32]*BrokerStatus),
		Topics:         make(map[string]*TopicState),
		ConsumerGroups: make(map[string]*ConsumerGroupState),
		Offsets:        make(map[string]int64),
	}

	for k, v := range f.state.Brokers {
		res.Brokers[k] = &BrokerStatus{
			ID:       v.ID,
			Host:     v.Host,
			Port:     v.Port,
			LastSeen: v.LastSeen,
			Active:   v.Active,
		}
	}

	// Filter topics if requested
	filter := len(topics) > 0
	topicMap := make(map[string]bool)
	for _, t := range topics {
		topicMap[t] = true
	}

	for k, v := range f.state.Topics {
		if filter && !topicMap[k] {
			continue
		}
		tState := &TopicState{
			Name:       v.Name,
			Partitions: make(map[uint32]*PartitionState),
		}
		for pID, pVal := range v.Partitions {
			replicaOffsetsCopy := make(map[uint32]int64)
			for rk, rv := range pVal.ReplicaOffsets {
				replicaOffsetsCopy[rk] = rv
			}
			tState.Partitions[pID] = &PartitionState{
				PartitionID:    pVal.PartitionID,
				LeaderID:       pVal.LeaderID,
				ReplicaIDs:     append([]uint32(nil), pVal.ReplicaIDs...),
				ISR:            append([]uint32(nil), pVal.ISR...),
				HighWatermark:  pVal.HighWatermark,
				ReplicaOffsets: replicaOffsetsCopy,
			}
		}
		res.Topics[k] = tState
	}

	for k, v := range f.state.ConsumerGroups {
		cgState := &ConsumerGroupState{
			GroupID:     v.GroupID,
			Generation:  v.Generation,
			Members:     make(map[string]*GroupMember),
			Assignments: make(map[string][]PartitionTopic),
		}
		for mk, mv := range v.Members {
			cgState.Members[mk] = &GroupMember{
				ID:       mv.ID,
				Topics:   append([]string(nil), mv.Topics...),
				LastSeen: mv.LastSeen,
			}
		}
		for ak, av := range v.Assignments {
			cgState.Assignments[ak] = append([]PartitionTopic(nil), av...)
		}
		res.ConsumerGroups[k] = cgState
	}

	for k, v := range f.state.Offsets {
		res.Offsets[k] = v
	}

	return res
}

func (f *FSM) GetOffset(groupID string, topic string, partition uint32) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := groupID + "/" + topic + "/" + string(rune(partition))
	if val, exists := f.state.Offsets[key]; exists {
		return val
	}
	return -1
}

// Snapshot FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	// Create a backup of state
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{stateData: data}, nil
}

// Restore FSM state from snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	
	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}

type fsmSnapshot struct {
	stateData []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if _, err := sink.Write(s.stateData); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (s *fsmSnapshot) Release() {}
