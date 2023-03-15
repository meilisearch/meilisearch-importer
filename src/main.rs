use byte_unit::{Byte, ByteError};
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use std::{
    error::Error,
    fs::{self, File},
    io::{self, prelude::*, BufRead},
    path::PathBuf,
};
use structopt::StructOpt;
///
/// An application that chunck the incoming file in packet of 10Mb and send them to a Meilisearch.
///

#[derive(Debug, StructOpt)]
#[structopt(
    name = "importer",
    about = "Could import any kind of data into Meilisearch"
)]
struct Opt {
    #[structopt(long)]
    url: String,

    #[structopt(long)]
    token: String,

    #[structopt(parse(from_os_str))]
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
    fn from_path(path: &PathBuf) -> Option<Mime> {
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
    reader: io::BufReader<File>,
    buffer: Vec<u8>,
    size: usize,
}

impl NdJsonChunker {
    fn new(file: PathBuf, size: usize) -> Self {
        let reader = io::BufReader::new(File::open(file).unwrap());
        Self {
            reader,
            buffer: Vec::new(),
            size,
        }
    }
}

impl Iterator for NdJsonChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        while let Ok(len) = self.reader.read_line(&mut line) {
            if len == 0 {
                return None;
            }
            if self.buffer.len() + len > self.size {
                let buffer = std::mem::replace(&mut self.buffer, Vec::new());
                return Some(buffer);
            } else {
                self.buffer.extend("\n".as_bytes());
                self.buffer.extend(line.as_bytes());
                line.clear();
            }
        }
        None
    }
}

struct CsvChunker {
    reader: io::BufReader<File>,
    buffer: Vec<u8>,
    size: usize,
    headers: Vec<u8>,
}

impl CsvChunker {
    fn new(file: PathBuf, size: usize) -> Self {
        let mut reader = io::BufReader::new(File::open(file).unwrap());
        let mut headers = String::new();
        reader.read_line(&mut headers).unwrap();
        Self {
            reader,
            buffer: Vec::new(),
            size,
            headers: headers.into_bytes(),
        }
    }
}

impl Iterator for CsvChunker {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        while let Ok(len) = self.reader.read_line(&mut line) {
            if len == 0 {
                return None;
            }
            if self.buffer.len() + len > self.size {
                let buffer = std::mem::replace(&mut self.buffer, self.headers.clone());
                return Some(buffer);
            } else {
                self.buffer.extend("\n".as_bytes());
                self.buffer.extend(line.as_bytes());
                line.clear();
            }
        }
        None
    }
}

async fn send_data(
    url: &str,
    token: &str,
    mime: &Mime,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

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
    // print!("{:?}", result);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();
    println!("{:?}", opt);

    // for each files present in the argument
    for file in opt.files {
        // check if the file exists
        if !file.exists() {
            return Err(format!("The file {:?} does not exist", file).into());
        }

        let mime = Mime::from_path(&file).expect("Could not find the mime type");
        let file_size = fs::metadata(&file)?.len();
        let size = opt.batch_size.get_bytes() as usize;
        let nb_chunks = file_size / size as u64;
        let pb = ProgressBar::new(nb_chunks);
        match mime {
            Mime::Json => {
                let data = fs::read_to_string(file)?;
                send_data(&opt.url, &opt.token, &mime, data.as_bytes()).await?;
            }
            Mime::NdJson => {
                let chunker = NdJsonChunker::new(file, size);
                for chunk in chunker {
                    send_data(&opt.url, &opt.token, &mime, &chunk).await?;
                    pb.inc(1);
                }
            }
            Mime::Csv => {
                let chunker = CsvChunker::new(file, size);
                for chunk in chunker {
                    send_data(&opt.url, &opt.token, &mime, &chunk).await?;
                    pb.inc(1);
                }
            }
        };
    }
    Ok(())
}
