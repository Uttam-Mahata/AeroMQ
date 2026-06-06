use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, error, debug};

use crate::log::LogManager;

pub struct DataServer {
    addr: SocketAddr,
    log_manager: Arc<LogManager>,
}

impl DataServer {
    pub fn new(addr: SocketAddr, log_manager: Arc<LogManager>) -> Self {
        Self { addr, log_manager }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("[AeroMQ Broker] Data Plane TCP Server listening on {}", self.addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let cpu_core = unsafe { libc::sched_getcpu() };
            info!("[AeroMQ Broker] Accepted connection from {} on CPU core {}", peer_addr, cpu_core);
            let log_manager = self.log_manager.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, log_manager).await {
                    error!("[AeroMQ Broker] Error handling connection from {}: {:?}", peer_addr, e);
                }
            });
        }
    }
}

async fn handle_connection(mut stream: TcpStream, log_manager: Arc<LogManager>) -> Result<(), Box<dyn std::error::Error>> {
    let mut header = [0u8; 7];

    loop {
        // Read Request Header: [magic (2 bytes)] [cmd (1 byte)] [body_len (4 bytes)]
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("[AeroMQ Broker] Client closed connection");
                break;
            }
            Err(e) => return Err(e.into()),
        }

        if header[0] != 0xAE || header[1] != 0x01 {
            return Err("Invalid protocol magic bytes".into());
        }

        let cmd = header[2];
        let body_len = u32::from_be_bytes(header[3..7].try_into().unwrap()) as usize;

        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;

        match cmd {
            1 => {
                // Command 1: Produce/Write
                // Payload structure: [topic_len (2 bytes)] [topic (string)] [partition (4 bytes)] [payload_len (4 bytes)] [payload (bytes)]
                if body.len() < 10 {
                    return Err("Produce body too short".into());
                }
                let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                if body.len() < 10 + topic_len {
                    return Err("Produce topic length exceeds body size".into());
                }
                let topic = String::from_utf8(body[2..2 + topic_len].to_vec())?;
                let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
                let payload_len = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap()) as usize;
                
                if body.len() < 10 + topic_len + payload_len {
                    return Err("Produce payload length mismatch".into());
                }
                let payload = &body[10 + topic_len..10 + topic_len + payload_len];

                // Append to partition log
                let part_log = log_manager.get_partition(&topic, partition).await?;
                let mut log_guard = part_log.lock().await;
                let offset = log_guard.append(payload)?;

                // Send Response: [magic (2)] [status (1: 0=Success)] [offset (8)]
                let mut resp = Vec::with_capacity(11);
                resp.extend_from_slice(&[0xAE, 0x01]);
                resp.push(0); // success
                resp.extend_from_slice(&offset.to_be_bytes());
                stream.write_all(&resp).await?;
            }
            2 => {
                // Command 2: Fetch/Read (Consumer fetch, respects High-Watermark)
                // Payload structure: [topic_len (2)] [topic (string)] [partition (4)] [start_offset (8)] [max_bytes (4)]
                if body.len() < 18 {
                    return Err("Fetch body too short".into());
                }
                let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                if body.len() < 18 + topic_len {
                    return Err("Fetch topic length exceeds body size".into());
                }
                let topic = String::from_utf8(body[2..2 + topic_len].to_vec())?;
                let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
                let start_offset = u64::from_be_bytes(body[6 + topic_len..14 + topic_len].try_into().unwrap());
                let max_bytes = u32::from_be_bytes(body[14 + topic_len..18 + topic_len].try_into().unwrap());

                // Read from partition log with High-Watermark blocking
                let part_log = log_manager.get_partition(&topic, partition).await?;
                
                let mut attempts = 0;
                let data_opt = loop {
                    let mut log_guard = part_log.lock().await;
                    let hw = log_guard.high_watermark;
                    
                    if start_offset < hw {
                        let res = log_guard.read_from_offset(start_offset, max_bytes)?;
                        break res;
                    }
                    
                    drop(log_guard);
                    attempts += 1;
                    if attempts >= 10 { // max 1 second wait (10 * 100ms)
                        break None;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                };
                
                match data_opt {
                    Some((file, position, bytes_to_read)) => {
                        // Send header indicating success with data
                        // Response: [magic (2)] [status (1: 2=Data)] [bytes_to_read (4)]
                        let mut resp = Vec::with_capacity(7);
                        resp.extend_from_slice(&[0xAE, 0x01]);
                        resp.push(2); // Success with Data
                        resp.extend_from_slice(&bytes_to_read.to_be_bytes());
                        stream.write_all(&resp).await?;

                        // Zero-Copy transfer of file segment to socket using sendfile
                        let socket_fd = stream.as_raw_fd();
                        let file_fd = file.as_raw_fd();

                        tokio::task::spawn_blocking(move || {
                            set_blocking(socket_fd, true)?;
                            let mut offset = position as libc::off_t;
                            let mut remaining = bytes_to_read as usize;

                            while remaining > 0 {
                                let written = unsafe {
                                    libc::sendfile(socket_fd, file_fd, &mut offset, remaining)
                                };
                                if written < 0 {
                                    let err = std::io::Error::last_os_error();
                                    let _ = set_blocking(socket_fd, false);
                                    return Err(err);
                                }
                                if written == 0 {
                                    break; // EOF or client disconnected
                                }
                                remaining -= written as usize;
                            }
                            set_blocking(socket_fd, false)?;
                            Ok::<_, std::io::Error>(())
                        }).await??;
                    }
                    None => {
                        // Send header indicating no data (status = 1)
                        let mut resp = Vec::with_capacity(3);
                        resp.extend_from_slice(&[0xAE, 0x01]);
                        resp.push(1); // Empty / No Data
                        stream.write_all(&resp).await?;
                    }
                }
            }
            3 => {
                // Command 3: Replica Fetch/Sync (Replica fetch, bypasses High-Watermark)
                // Payload structure: [replica_id (4)] [topic_len (2)] [topic (string)] [partition (4)] [start_offset (8)] [max_bytes (4)]
                if body.len() < 22 {
                    return Err("Replica Fetch body too short".into());
                }
                let replica_id = u32::from_be_bytes(body[0..4].try_into().unwrap());
                let topic_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
                if body.len() < 22 + topic_len {
                    return Err("Replica Fetch topic length exceeds body size".into());
                }
                let topic = String::from_utf8(body[6..6 + topic_len].to_vec())?;
                let partition = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap());
                let start_offset = u64::from_be_bytes(body[10 + topic_len..18 + topic_len].try_into().unwrap());
                let max_bytes = u32::from_be_bytes(body[18 + topic_len..22 + topic_len].try_into().unwrap());

                // Read from partition log (replicate/sync has no HW limit)
                let part_log = log_manager.get_partition(&topic, partition).await?;
                let mut log_guard = part_log.lock().await;
                
                // Record replica progress
                log_guard.update_follower_offset(replica_id, start_offset);

                match log_guard.read_from_offset(start_offset, max_bytes)? {
                    Some((file, position, bytes_to_read)) => {
                        let mut resp = Vec::with_capacity(7);
                        resp.extend_from_slice(&[0xAE, 0x01]);
                        resp.push(2); // Success with Data
                        resp.extend_from_slice(&bytes_to_read.to_be_bytes());
                        stream.write_all(&resp).await?;

                        let socket_fd = stream.as_raw_fd();
                        let file_fd = file.as_raw_fd();

                        tokio::task::spawn_blocking(move || {
                            set_blocking(socket_fd, true)?;
                            let mut offset = position as libc::off_t;
                            let mut remaining = bytes_to_read as usize;

                            while remaining > 0 {
                                let written = unsafe {
                                    libc::sendfile(socket_fd, file_fd, &mut offset, remaining)
                                };
                                if written < 0 {
                                    let err = std::io::Error::last_os_error();
                                    let _ = set_blocking(socket_fd, false);
                                    return Err(err);
                                }
                                if written == 0 {
                                    break;
                                }
                                remaining -= written as usize;
                            }
                            set_blocking(socket_fd, false)?;
                            Ok::<_, std::io::Error>(())
                        }).await??;
                    }
                    None => {
                        let mut resp = Vec::with_capacity(3);
                        resp.extend_from_slice(&[0xAE, 0x01]);
                        resp.push(1); // Empty / No Data
                        stream.write_all(&resp).await?;
                    }
                }
            }
            _ => return Err(format!("Unknown protocol command: {}", cmd).into()),
        }
    }

    Ok(())
}

fn set_blocking(fd: std::os::unix::io::RawFd, blocking: bool) -> std::io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let new_flags = if blocking {
            flags & !libc::O_NONBLOCK
        } else {
            flags | libc::O_NONBLOCK
        };
        if libc::fcntl(fd, libc::F_SETFL, new_flags) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
