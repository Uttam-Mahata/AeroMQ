package consensus

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

type RaftNode struct {
	Raft   *raft.Raft
	FSM    *FSM
	NodeID string
}

func NewRaftNode(nodeID string, raftAddr string, dataDir string, bootstrap bool) (*RaftNode, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	// Accelerate timeouts for local development/simulation
	config.HeartbeatTimeout = 200 * time.Millisecond
	config.ElectionTimeout = 200 * time.Millisecond
	config.LeaderLeaseTimeout = 150 * time.Millisecond
	config.CommitTimeout = 50 * time.Millisecond

	// Setup TCP transport
	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft addr %s: %w", raftAddr, err)
	}
	
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create tcp transport: %w", err)
	}

	fsm := NewFSM()

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	
	var snapshotStore raft.SnapshotStore
	if dataDir != "" {
		err = os.MkdirAll(dataDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create data dir: %w", err)
		}
		snapshotStore, err = raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
		if err != nil {
			return nil, fmt.Errorf("failed to create file snapshot store: %w", err)
		}
	} else {
		snapshotStore = raft.NewDiscardSnapshotStore()
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to construct raft node: %w", err)
	}

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return &RaftNode{
		Raft:   r,
		FSM:    fsm,
		NodeID: nodeID,
	}, nil
}

// Propose applies a state change to the Raft consensus group.
// It will fail if this node is not currently the cluster leader.
func (rn *RaftNode) Propose(op string, payload interface{}) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader (current leader is %s)", rn.Raft.Leader())
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	cmd := Command{
		Op:      op,
		Payload: rawPayload,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rn.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply error: %w", err)
	}

	res := future.Response()
	if err, ok := res.(error); ok && err != nil {
		return err
	}

	return nil
}

// Join adds a new node to the cluster.
func (rn *RaftNode) Join(nodeID string, addr string) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("cannot join node: not the leader")
	}

	future := rn.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter %s (%s): %w", nodeID, addr, err)
	}

	return nil
}
