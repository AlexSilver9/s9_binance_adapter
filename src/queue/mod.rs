use std::cmp::min;
use std::collections::VecDeque;
use std::path::PathBuf;
use s9_parquet::{ParquetWriter, Record};

pub struct QueuedParquetWriter {
    writer: ParquetWriter,
    queue: VecDeque<Record>,
    pub max_records_per_parquet_group: usize,
}

impl QueuedParquetWriter {
    pub fn new(file_path: &str, max_records_per_parquet_group: usize) -> anyhow::Result<Self, Box<dyn std::error::Error>> {
        let writer = ParquetWriter::new(file_path, max_records_per_parquet_group)?;
        let queue = VecDeque::with_capacity(max_records_per_parquet_group);
        Ok(Self {
            writer,
            queue,
            max_records_per_parquet_group
        })
    }

    pub fn enqueue(&mut self, record: Record) {
        self.queue.push_back(record)
    }

    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.max_records_per_parquet_group
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn file_path(&self) -> &PathBuf {
        &self.writer.file_path
    }

    pub fn flush(&mut self) -> anyhow::Result<usize, Box<dyn std::error::Error>> {
        let records: Vec<Record> = self.queue.drain(0..min(self.queue.len(), self.max_records_per_parquet_group)).collect();
        let written_records = records.len();
        match self.writer.write(&*records) {
            Ok(_) => Ok(written_records),
            Err(err) => Err(err.into())
        }
    }

    pub fn close(mut self) -> anyhow::Result<(), Box<dyn std::error::Error>> {
        self.writer.close()
    }
}