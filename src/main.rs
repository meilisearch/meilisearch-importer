use indicatif::ProgressBar;
use std::{
    error::Error,
    fs::{self, File},
    io::{self, BufRead},
    path::PathBuf,
};
use structopt::StructOpt;

///
/// An application that chunck the incoming file in packet of 10Mb and send them to a Meilisearch.

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
    file: PathBuf,
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
            Some("ndjson") => Some(Mime::NdJson),
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
    client
        .post(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", mime.as_str())
        .body(data.to_vec())
        .send()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();
    let mime = Mime::from_path(&opt.file).expect("Could not find the mime type");
    let file_size = fs::metadata(&opt.file)?.len();
    let size = 9 * 1024 * 1024;
    let nb_chunks = file_size / size as u64;
    let pb = ProgressBar::new(nb_chunks);
    match mime {
        Mime::Json => unimplemented!(),
        Mime::NdJson => {
            let chunker = NdJsonChunker::new(opt.file, size);
            for chunk in chunker {
                send_data(&opt.url, &opt.token, &mime, &chunk).await?;
                pb.inc(1);
            }
        }
        Mime::Csv => {
            let chunker = CsvChunker::new(opt.file, size);
            for chunk in chunker {
                send_data(&opt.url, &opt.token, &mime, &chunk).await?;
                pb.inc(1);
            }
        }
    };
    Ok(())
}
