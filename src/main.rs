use byte_count::ByteCount;
use byte_unit::Byte;
use csv::ByteRecord;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use serde_json::{de::IoRead, to_writer, Deserializer, Map, StreamDeserializer, Value};
use std::{
    error::Error,
    fs::{self, File},
    io::{self, prelude::*},
    mem,
    path::{Path, PathBuf},
};
use structopt::StructOpt;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

mod byte_count;

///
/// An application that chunck the incoming file in packet of 10Mb and send them to a Meilisearch.
///

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "importer",
    about = "Could import any kind of data into Meilisearch"
)]
struct Opt {
    #[structopt(long)]
    url: String,

    #[structopt(long)]
    index: String,

    #[structopt(long)]
    primary_key: Option<String>,

    #[structopt(long)]
    token: String,

    #[structopt(long, parse(from_os_str))]
    files: Vec<PathBuf>,

    // Get the batch size in bytes
    #[structopt(long, default_value = "90 MB", parse(try_from_str = Byte::from_str))]
    batch_size: Byte,
}

enum Mime {
    Json,
    NdJson,
    Csv,
}

impl Mime {
    fn from_path(path: &Path) -> Option<Mime> {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => Some(Mime::Json),
            Some("ndjson" | "jsonl") => Some(Mime::NdJson),
            Some("csv") => Some(Mime::Csv),
            _ => None,
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Mime::Json => "application/json",
            Mime::NdJson => "application/x-ndjson",
            Mime::Csv => "text/csv",
        }
    }
}

struct NdJsonChunker {
    reader: StreamDeserializer<'static, IoRead<io::BufReader<File>>, Map<String, Value>>,
    buffer: Vec<u8>,
    size: usize,
}

impl NdJsonChunker {
    fn new(file: PathBuf, size: usize) -> Self {
        let reader = io::BufReader::new(File::open(file).unwrap());
        Self {
            reader: Deserializer::from_reader(reader).into_iter(),
            buffer: Vec::new(),
            size,
        }
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

struct CsvChunker {
    reader: csv::Reader<File>,
    headers: ByteRecord,
    buffer: Vec<u8>,
    record: ByteRecord,
    size: usize,
}

impl CsvChunker {
    fn new(file: PathBuf, size: usize) -> Self {
        let mut reader = csv::Reader::from_path(file).unwrap();
        let mut buffer = Vec::new();
        let headers = reader.byte_headers().unwrap().clone();
        buffer.extend_from_slice(headers.as_slice());
        buffer.push(b'\n');
        Self {
            reader,
            headers,
            buffer,
            record: ByteRecord::new(),
            size,
        }
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

async fn send_data(opt: &Opt, mime: &Mime, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let token = opt.token.clone();
    let mut url = format!("{}/indexes/{}/documents", opt.url, opt.index);
    if let Some(primary_key) = &opt.primary_key {
        url = format!("{}?primaryKey={}", url, primary_key);
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    let data = encoder.finish()?;

    let result = client
        .post(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", mime.as_str())
        .header("Content-Encoding", "gzip")
        .body(data.to_vec())
        .send()
        .await?;

    if !result.status().is_success() {
        let text = result.text().await?;
        return Err(text.into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();
    let files = opt.files.clone();

    // for each files present in the argument
    for file in files {
        // check if the file exists
        if !file.exists() {
            return Err(format!("The file {:?} does not exist", file).into());
        }

        let mime = Mime::from_path(&file).expect("Could not find the mime type");
        let file_size = fs::metadata(&file)?.len();
        let size = opt.batch_size.get_bytes() as usize;
        let nb_chunks = file_size / size as u64;
        let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter).take(100);
        let pb = ProgressBar::new(nb_chunks);
        pb.inc(0);

        match mime {
            Mime::Json => {
                let data = fs::read_to_string(file)?;
                Retry::spawn(retry_strategy.clone(), || {
                    send_data(&opt, &mime, data.as_bytes())
                })
                .await?;
            }
            Mime::NdJson => {
                for chunk in NdJsonChunker::new(file, size) {
                    Retry::spawn(retry_strategy.clone(), || send_data(&opt, &mime, &chunk)).await?;
                    pb.inc(1);
                }
            }
            Mime::Csv => {
                for chunk in CsvChunker::new(file, size) {
                    Retry::spawn(retry_strategy.clone(), || send_data(&opt, &mime, &chunk)).await?;
                    pb.inc(1);
                }
            }
        };
    }
    Ok(())
}
