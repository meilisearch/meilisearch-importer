use std::fs::File;
use std::io::{self, Read};
use std::mem;
use std::path::{Path, PathBuf};

use csv::{ByteRecord, WriterBuilder};

pub struct CsvChunker {
    pub(crate) reader: csv::Reader<Box<dyn Read>>,
    pub(crate) headers: ByteRecord,
    pub(crate) writer: csv::Writer<Vec<u8>>,
    pub(crate) record_count: usize,
    pub(crate) record: ByteRecord,
    pub(crate) size: usize,
    pub(crate) delimiter: u8,
}

impl CsvChunker {
    pub fn new(path: PathBuf, size: usize, delimiter: u8) -> Self {
        let reader = if path == Path::new("-") {
            Box::new(io::stdin()) as Box<dyn Read>
        } else {
            Box::new(File::open(path).unwrap())
        };
        let mut reader = csv::Reader::from_reader(reader);
        let mut writer = WriterBuilder::new().delimiter(delimiter).from_writer(Vec::new());
        let headers = reader.byte_headers().unwrap().clone();
        writer.write_byte_record(&headers).unwrap();
        Self {
            reader,
            headers,
            writer,
            record_count: 0,
            record: ByteRecord::new(),
            size,
            delimiter,
        }
    }
}

impl Iterator for CsvChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.reader.read_byte_record(&mut self.record).unwrap() {
            self.writer.flush().unwrap();
            if self.writer.get_ref().len() + self.record.len() >= self.size {
                let mut writer =
                    WriterBuilder::new().delimiter(self.delimiter).from_writer(Vec::new());
                writer.write_byte_record(&self.headers).unwrap();
                self.record_count = 0;
                let writer = mem::replace(&mut self.writer, writer);

                // Insert the header and out of bound record
                self.writer.write_byte_record(&self.headers).unwrap();
                self.writer.write_byte_record(&self.record).unwrap();
                self.record_count += 1;

                return Some(writer.into_inner().unwrap());
            } else {
                // Insert only the record
                self.writer.write_byte_record(&self.record).unwrap();
                self.record_count += 1;
            }
        }
        if self.record_count == 0 {
            None
        } else {
            let mut writer = WriterBuilder::new().delimiter(self.delimiter).from_writer(Vec::new());
            writer.write_byte_record(&self.headers).unwrap();
            self.record_count = 0;
            // We make the buffer empty by doing that and next time we will
            // come back to this _if else_ condition to then return None.
            let writer = mem::replace(&mut self.writer, writer);
            Some(writer.into_inner().unwrap())
        }
    }
}
