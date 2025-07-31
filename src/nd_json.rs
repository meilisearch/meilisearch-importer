use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::{io, mem};

use serde_json::de::IoRead;
use serde_json::{to_writer, Deserializer, Map, StreamDeserializer, Value};

use crate::byte_count::ByteCount;

pub struct NdJsonChunker {
    #[allow(clippy::type_complexity)]
    pub reader: StreamDeserializer<'static, IoRead<BufReader<Box<dyn Read>>>, Map<String, Value>>,
    pub buffer: Vec<u8>,
    pub size: usize,
    pub ignore_embeddings: bool,
}

impl NdJsonChunker {
    pub fn new(path: PathBuf, size: usize, ignore_embeddings: bool) -> Self {
        let reader = if path == Path::new("-") {
            Box::new(io::stdin()) as Box<dyn Read>
        } else {
            Box::new(File::open(path).unwrap())
        };
        let reader = BufReader::new(reader);
        Self {
            reader: Deserializer::from_reader(reader).into_iter(),
            buffer: Vec::new(),
            size,
            ignore_embeddings,
        }
    }
}

impl Iterator for NdJsonChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in self.reader.by_ref() {
            let mut object = result.unwrap();

            if self.ignore_embeddings {
                object.remove("_vectors");
            }

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
