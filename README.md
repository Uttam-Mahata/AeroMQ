# AeroMQ

A distributed message queue system featuring a Go-based control plane and a Rust-based data plane. AeroMQ is designed for reliability, scalability, and extreme performance, utilizing Raft consensus for metadata management and a specialized tiered storage engine.

## 🚀 Key Features

- **Multi-language Architecture**: 
  - **Control Plane (Go)**: Leveraging Go's concurrency primitives and gRPC for robust cluster management.
  - **Data Plane (Rust)**: High-performance TCP message handling with CPU affinity and thread pinning. It utilizes:
    - **Zero-Copy I/O**: Leverages Linux `sendfile` to transfer data directly from page cache to network sockets.
    - **Memory-Mapped Indexing**: Uses `mmap` for blazing-fast offset lookups.
    - **Async I/O**: Powered by `tokio` and designed for `io_uring` compatibility to handle massive concurrency with minimal overhead.
- **Reliable Consensus**: Built-in **Raft** implementation for high availability, automatic leader election, and consistent metadata across the cluster.
- **Tiered Storage Architecture**:
  - **Hot Storage**: Rapid access to active log segments stored on local disk via append-only logs.
  - **Cold Storage**: Automatic rollover of older segments to secondary storage, ensuring infinite retention possibilities without impacting hot-path performance.
- **Full-featured Metadata Management**: Supports Topics, Partitions, ISR (In-Sync Replicas), and Replica assignments.
- **Consumer Group Support**: Dynamic group membership and offset management.
- **Integrated CLI Client**: Easy-to-use tool for administrative tasks and message operations.

## 🏗 Architecture

AeroMQ separates the control plane from the data plane to achieve maximum efficiency:

1.  **Controller (Go)**:
    *   Maintains the global cluster state.
    *   Handles broker registration and health monitoring.
    *   Orchestrates partition leader elections and rebalancing.
    *   Provides gRPC services for metadata discovery.
2.  **Broker (Rust)**:
    *   Manages physical log files (commit logs).
    *   Handles high-throughput TCP connections for message ingestion and retrieval.
    *   Performs log segment rollover and archival to cold storage.
3.  **Client (Go)**:
    *   Discovers topology via the Controller.
    *   Interacts directly with Brokers for data operations.

## 🛠 Prerequisites

- **Go**: 1.19 or higher
- **Rust**: 1.70 or higher (Cargo/Rustc)
- **Make**: For build orchestration
- **curl**: For health checks

## 📦 Build Instructions

Build all components (Controller, Broker, and Client) with a single command:

```bash
make build
```

This will produce:
- `go-controller/bin/controller`
- `rust-broker/target/release/rust-broker`
- `client/bin/client`

## 🚦 Getting Started

### 1. Launch a Local Cluster
AeroMQ includes a pre-configured 5-node setup (3 Controllers, 2 Brokers) for local development:

```bash
make start
```

### 2. Check Cluster Status
Verify that all nodes are healthy and the Raft consensus is active:

```bash
make status
```

### 3. Use the CLI Client
The client allows you to interact with the cluster:

**Create a Topic:**
```bash
./client/bin/client create-topic my-topic 3 2
```

**Produce a Message:**
```bash
./client/bin/client produce my-topic 0 "Hello, AeroMQ!"
```

**Consume Messages:**
```bash
./client/bin/client consume my-topic 0 0 --follow
```

**Fetch Cluster Metadata:**
```bash
./client/bin/client metadata
```

## 📁 Project Structure

- `go-controller/`: Go source code for the control plane and gRPC services.
- `rust-broker/`: Rust source code for the high-performance storage engine.
- `client/`: CLI client implementation in Go.
- `proto/`: Protobuf definitions for cross-service communication.
- `data/`: Local storage directory for log segments and snapshots.

## 🧪 Testing

AeroMQ includes a comprehensive integration test suite to verify end-to-end functionality:

```bash
./client/bin/client integration-test
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
