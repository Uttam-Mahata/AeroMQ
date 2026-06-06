use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Clone, Debug)]
pub struct LogSegment {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub idx_path: PathBuf,
}

pub struct PartitionLog {
    pub topic: String,
    pub partition: u32,
    pub partition_dir: PathBuf,
    pub segments: Vec<LogSegment>,
    pub active_log_file: File,
    pub active_idx_file: File,
    pub next_offset: u64,

    // Configurations
    pub max_segment_size: u64,
    pub broker_id: u32,
    pub max_retention_size: Option<u64>,
    pub max_retention_age: Option<std::time::Duration>,

    // High-Watermark and replica tracking
    pub high_watermark: u64,
    pub replica_offsets: HashMap<u32, u64>,
    pub replica_ids: Vec<u32>,
}

impl PartitionLog {
    pub fn new(
        base_dir: &Path,
        topic: &str,
        partition: u32,
        broker_id: u32,
        max_segment_size: u64,
        max_retention_size: Option<u64>,
        max_retention_age: Option<std::time::Duration>,
    ) -> io::Result<Self> {
        let partition_dir = base_dir
            .join(topic)
            .join(format!("partition_{}", partition));
        
        fs::create_dir_all(&partition_dir)?;

        // Migration step: rename old style files if they exist
        let old_log = partition_dir.join("partition.log");
        let old_idx = partition_dir.join("partition.idx");
        if old_log.exists() {
            let new_log = partition_dir.join(format!("{:020}.log", 0));
            let new_idx = partition_dir.join(format!("{:020}.idx", 0));
            fs::rename(&old_log, &new_log)?;
            if old_idx.exists() {
                fs::rename(&old_idx, &new_idx)?;
            }
        }

        // Discover segments
        let mut base_offsets = Vec::new();
        for entry in fs::read_dir(&partition_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "log" {
                        if let Some(stem) = path.file_stem() {
                            if let Some(name_str) = stem.to_str() {
                                if name_str.len() == 20 {
                                    if let Ok(offset) = name_str.parse::<u64>() {
                                        base_offsets.push(offset);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        base_offsets.sort_unstable();

        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let mut segments = Vec::new();
        for &offset in &base_offsets {
            let log_path = partition_dir.join(format!("{:020}.log", offset));
            let idx_path = partition_dir.join(format!("{:020}.idx", offset));
            segments.push(LogSegment {
                base_offset: offset,
                log_path,
                idx_path,
            });
        }

        let active_seg = segments.last().unwrap();
        let active_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&active_seg.log_path)?;

        let mut active_idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&active_seg.idx_path)?;

        // Determine next offset from the last index entry of active segment
        let index_len = active_idx_file.metadata()?.len();
        let next_offset = if index_len >= 16 {
            active_idx_file.seek(SeekFrom::Start(index_len - 16))?;
            let mut buf = [0u8; 16];
            active_idx_file.read_exact(&mut buf)?;
            u64::from_be_bytes(buf[0..8].try_into().unwrap()) + 1
        } else {
            active_seg.base_offset
        };

        // Put seek back to end
        active_idx_file.seek(SeekFrom::End(0))?;

        let mut log = Self {
            topic: topic.to_string(),
            partition,
            partition_dir,
            segments,
            active_log_file,
            active_idx_file,
            next_offset,
            max_segment_size,
            broker_id,
            max_retention_size,
            max_retention_age,
            high_watermark: 0,
            replica_offsets: HashMap::new(),
            replica_ids: Vec::new(),
        };
        log.recompute_high_watermark();
        Ok(log)
    }

    pub fn update_follower_offset(&mut self, replica_id: u32, offset: u64) {
        self.replica_offsets.insert(replica_id, offset);
        self.recompute_high_watermark();
    }

    pub fn recompute_high_watermark(&mut self) {
        let other_replicas: Vec<u32> = self.replica_ids.iter().copied().filter(|&id| id != self.broker_id).collect();
        if other_replicas.is_empty() {
            self.high_watermark = self.next_offset;
            return;
        }

        let mut min_offset = self.next_offset;
        for rep_id in other_replicas {
            let rep_offset = self.replica_offsets.get(&rep_id).copied().unwrap_or(0);
            if rep_offset < min_offset {
                min_offset = rep_offset;
            }
        }
        self.high_watermark = min_offset;
    }

    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let cur_len = self.active_log_file.metadata()?.len();
        if cur_len + data.len() as u64 > self.max_segment_size && cur_len > 0 {
            self.roll_over()?;
        }

        let pos = self.active_log_file.metadata()?.len();
        // Seek to end before writing (append mode manually simulated for flexibility)
        self.active_log_file.seek(SeekFrom::End(0))?;
        self.active_log_file.write_all(data)?;
        self.active_log_file.flush()?;

        let offset = self.next_offset;
        self.active_idx_file.seek(SeekFrom::End(0))?;
        self.active_idx_file.write_all(&offset.to_be_bytes())?;
        self.active_idx_file.write_all(&pos.to_be_bytes())?;
        self.active_idx_file.flush()?;

        self.next_offset += 1;

        // Clean up retention
        self.clean_retention()?;

        self.recompute_high_watermark();
        Ok(offset)
    }

    fn roll_over(&mut self) -> io::Result<()> {
        self.active_log_file.flush()?;
        self.active_idx_file.flush()?;

        let old_active_seg = self.segments.last().unwrap().clone();

        // Copy old segment to cold storage
        let cold_dir = PathBuf::from(format!("./data/cold_storage/broker_{}", self.broker_id))
            .join(&self.topic)
            .join(format!("partition_{}", self.partition));
        
        fs::create_dir_all(&cold_dir)?;
        
        let cold_log_path = cold_dir.join(format!("{:020}.log", old_active_seg.base_offset));
        let cold_idx_path = cold_dir.join(format!("{:020}.idx", old_active_seg.base_offset));
        
        fs::copy(&old_active_seg.log_path, &cold_log_path)?;
        fs::copy(&old_active_seg.idx_path, &cold_idx_path)?;
        info!(
            "[AeroMQ Broker] Segment {} rolled over and copied to cold storage: {:?}",
            old_active_seg.base_offset, cold_log_path
        );

        // Create new active segment
        let new_base_offset = self.next_offset;
        let new_log_path = self.partition_dir.join(format!("{:020}.log", new_base_offset));
        let new_idx_path = self.partition_dir.join(format!("{:020}.idx", new_base_offset));

        let new_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&new_log_path)?;

        let new_idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&new_idx_path)?;

        self.active_log_file = new_log_file;
        self.active_idx_file = new_idx_file;

        self.segments.push(LogSegment {
            base_offset: new_base_offset,
            log_path: new_log_path,
            idx_path: new_idx_path,
        });

        Ok(())
    }

    fn clean_retention(&mut self) -> io::Result<()> {
        if let Some(max_size) = self.max_retention_size {
            while self.segments.len() > 1 {
                let mut total_size = 0;
                for seg in &self.segments {
                    if let Ok(meta) = fs::metadata(&seg.log_path) {
                        total_size += meta.len();
                    }
                    if let Ok(meta) = fs::metadata(&seg.idx_path) {
                        total_size += meta.len();
                    }
                }

                if total_size <= max_size {
                    break;
                }

                let oldest = self.segments.remove(0);
                let _ = fs::remove_file(&oldest.log_path);
                let _ = fs::remove_file(&oldest.idx_path);
                info!(
                    "[AeroMQ Broker] Retention cleaner: total directory size ({}) exceeded limit ({}). Removed local segment {}",
                    total_size, max_size, oldest.base_offset
                );
            }
        }

        if let Some(max_age) = self.max_retention_age {
            let now = std::time::SystemTime::now();
            while self.segments.len() > 1 {
                let oldest_log = &self.segments[0].log_path;
                let should_remove = if let Ok(meta) = fs::metadata(oldest_log) {
                    if let Ok(modified) = meta.modified() {
                        if let Ok(duration) = now.duration_since(modified) {
                            duration > max_age
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if !should_remove {
                    break;
                }

                let oldest = self.segments.remove(0);
                let _ = fs::remove_file(&oldest.log_path);
                let _ = fs::remove_file(&oldest.idx_path);
                info!(
                    "[AeroMQ Broker] Retention cleaner: age threshold exceeded. Removed local segment {}",
                    oldest.base_offset
                );
            }
        }

        Ok(())
    }

    fn find_cold_segment(&self, start_offset: u64) -> io::Result<Option<LogSegment>> {
        let cold_dir = PathBuf::from(format!("./data/cold_storage/broker_{}", self.broker_id))
            .join(&self.topic)
            .join(format!("partition_{}", self.partition));

        if !cold_dir.exists() {
            return Ok(None);
        }

        let mut base_offsets = Vec::new();
        for entry in fs::read_dir(&cold_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "log" {
                        if let Some(stem) = path.file_stem() {
                            if let Some(name_str) = stem.to_str() {
                                if name_str.len() == 20 {
                                    if let Ok(offset) = name_str.parse::<u64>() {
                                        base_offsets.push(offset);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if base_offsets.is_empty() {
            return Ok(None);
        }

        base_offsets.sort_unstable();

        let mut low = 0;
        let mut high = base_offsets.len() - 1;
        let mut found_offset = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_base = base_offsets[mid];
            if mid_base <= start_offset {
                found_offset = Some(mid_base);
                low = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }

        if let Some(offset) = found_offset {
            let log_path = cold_dir.join(format!("{:020}.log", offset));
            let idx_path = cold_dir.join(format!("{:020}.idx", offset));
            Ok(Some(LogSegment {
                base_offset: offset,
                log_path,
                idx_path,
            }))
        } else {
            Ok(None)
        }
    }

    /// Read data starting from a specific offset.
    /// Returns the file handle, the starting position in the file, and the number of bytes to read.
    pub fn read_from_offset(&mut self, start_offset: u64, max_bytes: u32) -> io::Result<Option<(File, u64, u32)>> {
        let mut seg = None;

        if !self.segments.is_empty() && start_offset >= self.segments[0].base_offset {
            // Binary search local segments
            let mut low = 0;
            let mut high = self.segments.len() - 1;
            let mut found_seg_idx = None;

            while low <= high {
                let mid = low + (high - low) / 2;
                let mid_base = self.segments[mid].base_offset;
                if mid_base <= start_offset {
                    found_seg_idx = Some(mid);
                    low = mid + 1;
                } else {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
            }

            if let Some(idx) = found_seg_idx {
                seg = Some(self.segments[idx].clone());
            }
        }

        if seg.is_none() {
            // Try cold storage
            seg = self.find_cold_segment(start_offset)?;
        }

        let seg = match seg {
            Some(s) => s,
            None => return Ok(None),
        };

        // Binary search on the segment's index file
        let mut idx_file = File::open(&seg.idx_path)?;
        let index_len = idx_file.metadata()?.len();
        let num_entries = index_len / 16;
        if num_entries == 0 {
            return Ok(None);
        }

        let mut low = 0;
        let mut high = num_entries - 1;
        let mut found_idx = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            idx_file.seek(SeekFrom::Start(mid * 16))?;
            let mut buf = [0u8; 16];
            idx_file.read_exact(&mut buf)?;

            let offset = u64::from_be_bytes(buf[0..8].try_into().unwrap());
            
            if offset >= start_offset {
                found_idx = Some(mid);
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        let entry_idx = match found_idx {
            Some(idx) => idx,
            None => return Ok(None),
        };

        idx_file.seek(SeekFrom::Start(entry_idx * 16))?;
        let mut buf = [0u8; 16];
        idx_file.read_exact(&mut buf)?;
        let position = u64::from_be_bytes(buf[8..16].try_into().unwrap());

        let log_file = File::open(&seg.log_path)?;
        let log_len = log_file.metadata()?.len();
        let end_pos = if entry_idx + 1 < num_entries {
            idx_file.seek(SeekFrom::Start((entry_idx + 1) * 16))?;
            let mut next_buf = [0u8; 16];
            idx_file.read_exact(&mut next_buf)?;
            u64::from_be_bytes(next_buf[8..16].try_into().unwrap())
        } else {
            log_len
        };

        let bytes_to_read = std::cmp::min((end_pos - position) as u32, max_bytes);
        if bytes_to_read == 0 {
            return Ok(None);
        }

        Ok(Some((log_file, position, bytes_to_read)))
    }
}

pub struct LogManager {
    base_dir: PathBuf,
    pub broker_id: u32,
    pub max_segment_size: u64,
    pub max_retention_size: Option<u64>,
    pub max_retention_age: Option<std::time::Duration>,
    partitions: Mutex<HashMap<(String, u32), Arc<Mutex<PartitionLog>>>>,
}

impl LogManager {
    pub fn new<P: AsRef<Path>>(base_dir: P, broker_id: u32) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            broker_id,
            max_segment_size: 210, // 210 bytes default for testing/prototype segment rolling
            max_retention_size: Some(256 * 1024), // 256KB default
            max_retention_age: Some(std::time::Duration::from_secs(3600)), // 1 hour default
            partitions: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_limits(
        mut self,
        max_segment_size: u64,
        max_retention_size: Option<u64>,
        max_retention_age: Option<std::time::Duration>,
    ) -> Self {
        self.max_segment_size = max_segment_size;
        self.max_retention_size = max_retention_size;
        self.max_retention_age = max_retention_age;
        self
    }

    pub async fn get_all_offsets(&self) -> Vec<(String, u32, i64)> {
        let parts = self.partitions.lock().await;
        let mut offsets = Vec::new();
        for ((topic, partition), part_log_arc) in parts.iter() {
            let part_log = part_log_arc.lock().await;
            offsets.push((topic.clone(), *partition, part_log.next_offset as i64));
        }
        offsets
    }

    pub async fn get_partition(&self, topic: &str, partition: u32) -> io::Result<Arc<Mutex<PartitionLog>>> {
        let mut parts = self.partitions.lock().await;
        let key = (topic.to_string(), partition);
        if let Some(log) = parts.get(&key) {
            return Ok(log.clone());
        }

        let log = PartitionLog::new(
            &self.base_dir,
            topic,
            partition,
            self.broker_id,
            self.max_segment_size,
            self.max_retention_size,
            self.max_retention_age,
        )?;
        let shared = Arc::new(Mutex::new(log));
        parts.insert(key, shared.clone());
        Ok(shared)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_segmented_log_rollover_and_cold_storage() {
        let test_dir = PathBuf::from("./data/test_temp_segmented_log");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        // Clear existing cold storage for broker 99
        let cold_dir = PathBuf::from("./data/cold_storage/broker_99");
        let _ = fs::remove_dir_all(&cold_dir);

        let manager = LogManager::new(&test_dir, 99)
            .with_limits(100, Some(250), None); // small limits for testing

        let part_log_arc = manager.get_partition("test-topic", 0).await.unwrap();
        
        let mut log = part_log_arc.lock().await;
        
        // Append 40 bytes. Max segment size is 100 bytes.
        let off0 = log.append(&[1; 40]).unwrap();
        let off1 = log.append(&[2; 40]).unwrap();
        assert_eq!(off0, 0);
        assert_eq!(off1, 1);
        assert_eq!(log.segments.len(), 1);

        // This append should trigger rollover because 80 + 40 = 120 > 100
        let off2 = log.append(&[3; 40]).unwrap();
        assert_eq!(off2, 2);
        
        // We should have 2 segments now
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.segments[0].base_offset, 0);
        assert_eq!(log.segments[1].base_offset, 2);

        // Check that segment 0 exists in cold storage
        let cold_log_path = cold_dir
            .join("test-topic")
            .join("partition_0")
            .join(format!("{:020}.log", 0));
        assert!(cold_log_path.exists());

        // Write more to trigger retention deletion of segment 0 locally
        // Total retention limit is 250 bytes.
        let off3 = log.append(&[4; 40]).unwrap(); // segment 1 size = 80b
        let off4 = log.append(&[5; 40]).unwrap(); // rollover! segment 2 starts at offset 4
        let off5 = log.append(&[6; 40]).unwrap(); // segment 2 size = 80b

        // Local segments:
        // Segment 0 (offset 0): size is 80b log + 32b idx = 112b
        // Segment 2 (offset 2): size is 80b log + 32b idx = 112b
        // Segment 4 (offset 4): size is 80b log + 32b idx = 112b
        // Total = 336b > 250b limit. So segment 0 should be deleted locally.
        
        assert!(!log.segments.iter().any(|s| s.base_offset == 0));
        
        // But we should still be able to read offset 0 and 1 seamlessly from cold storage!
        let read_res = log.read_from_offset(0, 40).unwrap();
        assert!(read_res.is_some());
        let (mut file, pos, bytes) = read_res.unwrap();
        assert_eq!(bytes, 40);
        let mut buf = vec![0; bytes as usize];
        file.seek(SeekFrom::Start(pos)).unwrap();
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![1; 40]);

        // Clean up
        let _ = fs::remove_dir_all(&test_dir);
        let _ = fs::remove_dir_all(&cold_dir);
    }
}
