use std::path::{Path, PathBuf};
use s9_parquet::{ParquetWriter, Record};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::Duration;
use crossbeam_channel::{Receiver, Sender, unbounded, select};

pub enum ControlMessage {
    Flush,
    Shutdown,
}

pub struct ConcurrentQueuedParquetWriter {
    record_tx: Sender<Record>,
    control_tx: Sender<ControlMessage>,
    queue_size: Arc<AtomicUsize>,
    file_path: PathBuf,
    worker_handle: Option<thread::JoinHandle<()>>,
}

impl ConcurrentQueuedParquetWriter {
    pub fn new(
        file_path: &str,
        capacity: usize,
        flush_timeout: Duration,
        error_tx: Sender<(String, String)>,
    ) -> anyhow::Result<Self, Box<dyn std::error::Error>>
    {
        let (record_tx, record_rx) = unbounded::<Record>();
        let (control_tx, control_rx) = unbounded::<ControlMessage>();

        let writer = ParquetWriter::new(file_path, capacity)?;
        let queue_size = Arc::new(AtomicUsize::new(0));
        let queue_size_clone = queue_size.clone();
        let path_buf = PathBuf::from(file_path);
        let path_buf_clone = path_buf.clone();

        let worker_handle = thread::spawn(move || {
            worker_loop(
                writer,
                capacity,
                flush_timeout,
                record_rx,
                control_rx,
                error_tx,
                queue_size_clone,
                path_buf_clone,
            );
        });

        Ok(Self {
            record_tx,
            control_tx,
            queue_size,
            file_path: path_buf,
            worker_handle: Some(worker_handle),
        })
    }

    pub fn enqueue(&self, record: Record) -> Result<(), crossbeam_channel::SendError<Record>> {
        self.record_tx.send(record)?;
        self.queue_size.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    // Exists for convenience, to flush the writer immediately without waiting for the flush timeout.
    pub fn flush(&self) -> Result<(), crossbeam_channel::SendError<ControlMessage>> {
        self.control_tx.send(ControlMessage::Flush)
    }

    pub fn shutdown(mut self) -> thread::Result<()> {
        if let Some(handle) = self.worker_handle.take() {
            let _ = self.control_tx.send(ControlMessage::Shutdown);
            handle.join()
        } else {
            Ok(())
        }
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    pub fn queue_size(&self) -> usize {
        self.queue_size.load(Ordering::Relaxed)
    }
}

fn worker_loop(
    mut writer: ParquetWriter,
    capacity: usize,
    flush_timeout: Duration,
    record_rx: Receiver<Record>,
    control_rx: Receiver<ControlMessage>,
    error_tx: Sender<(String, String)>,
    queue_size: Arc<AtomicUsize>,
    file_path: PathBuf,
) {
    let mut queue: Vec<Record> = Vec::with_capacity(capacity);
    let mut running = true;
    let file_path = file_path.to_string_lossy().to_string();

    while running {
        select! {
            recv(record_rx) -> msg => match msg {
                Ok(record) => {
                    queue.push(record);
                    if queue.len() >= capacity {
                        flush_queue(&mut writer, &mut queue, &error_tx, &queue_size, &file_path);
                    }
                },
                Err(_) => {
                    // Crossbeam channel disconnected, initiate shutdown
                    println!("Worker for {:?} disconnected from record channel, shutting down", file_path);
                    running = false;
                }
            },
            recv(control_rx) -> msg => match msg {
                Ok(ControlMessage::Flush) => {
                    flush_queue(&mut writer, &mut queue, &error_tx, &queue_size, &file_path);
                },
                Ok(ControlMessage::Shutdown) => {
                    running = false;
                },
                Err(_) => {
                    // Crossbeam channel disconnected, initiate shutdown
                    println!("Worker for {:?} disconnected from control channel, shutting down", file_path);
                    running = false;
                }
            },
            default(flush_timeout) => {
                if !queue.is_empty() {
                    flush_queue(&mut writer, &mut queue, &error_tx, &queue_size, &file_path);
                }
            }
        }
    }

    // Final flush before shutting down
    if !queue.is_empty() {
        println!("Worker for {:?} shutting down, performing final flush of {} records...", file_path, queue.len());
        flush_queue(&mut writer, &mut queue, &error_tx, &queue_size, &file_path);
    }

    if let Err(e) = writer.close() {
        let _ = error_tx.send((file_path.to_string(), format!("Error closing ParquetWriter for {:?}: {}", file_path, e)));
    }
}

fn flush_queue(
    writer: &mut ParquetWriter,
    queue: &mut Vec<Record>,
    error_tx: &Sender<(String, String)>,
    queue_size: &Arc<AtomicUsize>,
    file_path: &str,
) {
    if queue.is_empty() {
        return;
    }

    let records_to_write: Vec<Record> = queue.drain(..).collect();
    let num_records = records_to_write.len();

    if let Err(e) = writer.write(&records_to_write) {
        let _ = error_tx.send((file_path.to_string(), format!("Error writing to Parquet file {:?}: {}", file_path, e)));
    }

    queue_size.fetch_sub(num_records, Ordering::Relaxed);
}