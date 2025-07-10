use std::fs::File;
use std::hash::{DefaultHasher, Hash};
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
    pub me: String,
    pub other: Vec<String>,
    pub pk: String,
}

impl NdJsonChunker {
    pub fn new(path: &PathBuf, size: usize, me: String, other: Vec<String>, pk: String) -> Self {
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
            me,
            other,
            pk,
        }
    }
}

impl Iterator for NdJsonChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in self.reader.by_ref() {
            let object = result.unwrap();
            let pk = object.get(&self.pk).unwrap().as_str().unwrap();
            let max_hash = self
                .other
                .iter()
                .filter(|other| *other != &self.me)
                .map(|url| format!("{url}{pk}").hash(&mut DefaultHasher::new()))
                .max()
                .unwrap();
            if max_hash > format!("{}{pk}", self.me).hash(&mut DefaultHasher::new()) {
                continue;
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
