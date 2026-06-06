use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;
use tracing::info;

mod log;
mod net;
mod grpc;

#[derive(Parser, Debug)]
#[command(author, version, about = "AeroMQ High-Performance Rust Broker")]
struct Args {
    /// Unique Broker ID
    #[arg(long, default_value_t = 1)]
    id: u32,

    /// Bind IP Address
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// TCP port for high-throughput client data operations
    #[arg(long, default_value_t = 9091)]
    data_port: i32,

    /// gRPC Endpoint of the Go Control Plane
    #[arg(long, default_value = "http://127.0.0.1:8001")]
    controller: String,

    /// Path to store physical partition log files
    #[arg(long)]
    storage_dir: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize premium logging format
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    let storage_dir = args.storage_dir.unwrap_or_else(|| {
        PathBuf::from(format!("./data/broker_{}", args.id))
    });

    info!("[AeroMQ Broker] Initializing Storage Broker {}...", args.id);
    info!("[AeroMQ Broker] Commit Log Storage: {:?}", storage_dir);

    // Build Tokio runtime with CPU affinity/thread pinning
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            static THREAD_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let tid = THREAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            
            let num_cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let core_id = tid % num_cores;

            unsafe {
                let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
                libc::CPU_SET(core_id, &mut cpuset);
                let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
                if ret == 0 {
                    tracing::info!("[AeroMQ Broker] Pinned worker thread {} to CPU core {}", tid, core_id);
                } else {
                    tracing::warn!("[AeroMQ Broker] Failed to pin worker thread {} to CPU core {}: {}", tid, core_id, std::io::Error::last_os_error());
                }
            }
        })
        .build()?;

    runtime.block_on(async move {
        let log_manager = Arc::new(log::LogManager::new(storage_dir, args.id));

        // Spawn client registration & control plane heartbeat worker
        let grpc_log_manager = log_manager.clone();
        let my_host = args.host.clone();
        let controller_addr = args.controller.clone();
        tokio::spawn(async move {
            grpc::run_control_plane_loop(
                args.id,
                my_host,
                args.data_port,
                controller_addr,
                grpc_log_manager,
            ).await;
        });

        // Run TCP Data Plane server
        let bind_addr: SocketAddr = format!("{}:{}", args.host, args.data_port).parse()?;
        let server = net::DataServer::new(bind_addr, log_manager);

        server.run().await?;

        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
