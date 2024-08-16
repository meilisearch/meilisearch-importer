use std::fs::File;
use std::mem;
use std::path::PathBuf;

use csv::{ReaderBuilder, ByteRecord};

pub struct CsvChunker {
    pub(crate) reader: csv::Reader<File>,
    pub(crate) headers: ByteRecord,
    pub(crate) buffer: Vec<u8>,
    pub(crate) record: ByteRecord,
    pub(crate) size: usize,
}

impl CsvChunker {
    pub fn new(file: PathBuf, size: usize) -> Self {
        let mut reader = ReaderBuilder::new()
            .flexible(true)
            .from_path(file)
            .unwrap();
        let mut buffer = Vec::new();
        let headers = reader.byte_headers().unwrap().clone();
        buffer.extend_from_slice(headers.as_slice());
        buffer.push(b'\n');
        Self { reader, headers, buffer, record: ByteRecord::new(), size }
    }
}

impl Iterator for CsvChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.reader.read_byte_record(&mut self.record).unwrap() {
            if self.buffer.len() + self.record.len() >= self.size {
                let buffer = mem::take(&mut self.buffer);

                // Insert the header and out of bound record
                self.buffer.extend_from_slice(self.headers.as_slice());
                self.buffer.push(b'\n');
                self.buffer.extend_from_slice(self.record.as_slice());
                self.buffer.push(b'\n');

                return Some(buffer);
            } else {
                // Insert only the record
                self.buffer.extend_from_slice(self.record.as_slice());
                self.buffer.push(b'\n');
            }
        }
        // If there only less than or the headers in the buffer and a
        // newline character it means that there are no documents in it.
        if self.buffer.len() <= self.headers.len() + 1 {
            None
        } else {
            // We make the buffer empty by doing that and next time we will
            // come back to this _if else_ condition to then return None.
            Some(mem::take(&mut self.buffer))
        }
    }
}
