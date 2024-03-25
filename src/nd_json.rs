use std::fs::File;
use std::path::PathBuf;
use std::{io, mem};

use serde_json::de::IoRead;
use serde_json::{to_writer, Deserializer, Map, StreamDeserializer, Value};

use crate::byte_count::ByteCount;

pub struct NdJsonChunker {
    pub reader: StreamDeserializer<'static, IoRead<io::BufReader<File>>, Map<String, Value>>,
    pub buffer: Vec<u8>,
    pub size: usize,
}

impl NdJsonChunker {
    pub fn new(file: PathBuf, size: usize) -> Self {
        let reader = io::BufReader::new(File::open(file).unwrap());
        Self { reader: Deserializer::from_reader(reader).into_iter(), buffer: Vec::new(), size }
    }
}

impl Iterator for NdJsonChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in self.reader.by_ref() {
            let object = result.unwrap();

            // Evaluate the size it will take if we serialize it in the buffer
            let mut counter = ByteCount::new();
            to_writer(&mut counter, &object).unwrap();

            if self.buffer.len() + counter.count() >= self.size {
                let buffer = mem::take(&mut self.buffer);
                // Insert the record but after we sent the buffer
                to_writer(&mut self.buffer, &object).unwrap();
                return Some(buffer);
            } else {
                // Insert the record
                to_writer(&mut self.buffer, &object).unwrap();
            }
        }
        if self.buffer.is_empty() {
            None
        } else {
            Some(mem::take(&mut self.buffer))
        }
    }
}
