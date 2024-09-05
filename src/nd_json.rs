use std::fs::File;
use std::path::PathBuf;
use std::{io, mem};

use serde_json::de::IoRead;
use serde_json::{to_writer, Deserializer, Map, StreamDeserializer, Value};

use crate::byte_count::ByteCount;
use anyhow::{Result};

pub struct NdJsonChunker {
    pub reader: StreamDeserializer<'static, IoRead<io::BufReader<File>>, Map<String, Value>>,
    pub buffer: Vec<u8>,
    pub size: usize,
}

impl NdJsonChunker {
    pub fn new(file: PathBuf, size: usize) -> Result<Self> {
        let reader = io::BufReader::new(File::open(file)?);
        Ok(Self { reader: Deserializer::from_reader(reader).into_iter(), buffer: Vec::new(), size })
    }
}

impl Iterator for NdJsonChunker {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in self.reader.by_ref() {
            let object = match result {
                Ok(obj) => obj,
                Err(e) => return Some(Err(e.into())),
            };

            // Evaluate the size it will take if we serialize it in the buffer
            let mut counter = ByteCount::new();
            if let Err(e) = to_writer(&mut counter, &object) {
                return Some(Err(e.into()));
            }

            if self.buffer.len() + counter.count() >= self.size {
                let buffer = mem::take(&mut self.buffer);
                // Insert the record but after we sent the buffer
                if let Err(e) = to_writer(&mut self.buffer, &object) {
                    return Some(Err(e.into()));
                }
                return Some(Ok(buffer));
            } else {
                // Insert the record
                if let Err(e) = to_writer(&mut self.buffer, &object) {
                    return Some(Err(e.into()));
                }
            }
        }
        if self.buffer.is_empty() {
            None
        } else {
            Some(Ok(mem::take(&mut self.buffer)))
        }
    }
}
