use std::fs::File;
use std::mem;
use std::path::PathBuf;

use anyhow::{Context, Result};
use csv::{ByteRecord, ReaderBuilder};

pub struct CsvChunker {
    pub(crate) reader: csv::Reader<File>,
    pub(crate) headers: ByteRecord,
    pub(crate) buffer: Vec<u8>,
    pub(crate) record: ByteRecord,
    pub(crate) size: usize,
}

impl CsvChunker {
    pub fn new(file: PathBuf, size: usize, delimiter: Option<char>) -> Result<Self> {
        let mut reader_builder = ReaderBuilder::new();
        if let Some(delim) = delimiter {
            reader_builder.delimiter(delim as u8);
        }
        let mut reader = reader_builder.from_path(&file)
            .with_context(|| format!("Failed to create CSV reader for file {:?}", file))?;
        
        let mut buffer = Vec::new();
        let headers = reader.byte_headers()
            .with_context(|| "Failed to read CSV headers")?
            .clone();
        buffer.extend_from_slice(headers.as_slice());
        buffer.push(b'\n');
        Ok(Self { 
            reader, 
            headers, 
            buffer, 
            record: ByteRecord::new(), 
            size,
        })
    }
}

impl Iterator for CsvChunker {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_byte_record(&mut self.record) {
                Ok(true) => {
                    if self.buffer.len() + self.record.len() >= self.size {
                        let buffer = mem::take(&mut self.buffer);

                        // Insert the header and out of bound record
                        self.buffer.extend_from_slice(self.headers.as_slice());
                        self.buffer.push(b'\n');
                        self.buffer.extend_from_slice(self.record.as_slice());
                        self.buffer.push(b'\n');

                        return Some(Ok(buffer));
                    } else {
                        // Insert only the record
                        self.buffer.extend_from_slice(self.record.as_slice());
                        self.buffer.push(b'\n');
                    }
                },
                Ok(false) => {
                    // End of file reached
                    if self.buffer.len() <= self.headers.len() + 1 {
                        return None;
                    } else {
                        return Some(Ok(mem::take(&mut self.buffer)));
                    }
                },
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}
