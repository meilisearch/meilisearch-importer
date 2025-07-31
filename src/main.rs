use std::io::prelude::*;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::time::Duration;
use std::{fs, thread};

use anyhow::Context;
use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use exponential_backoff::Backoff;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use mime::Mime;
use rayon::iter::{ParallelBridge as _, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};
use ureq::{Agent, AgentBuilder};

mod byte_count;
mod csv;
mod mime;
mod nd_json;

/// A tool to import massive datasets into Meilisearch by sending them in batches.
#[derive(Debug, Parser, Clone)]
#[command(name = "meilisearch-importer")]
struct Opt {
    /// The URL of your instance. You can find it on the main project page on the Cloud.
    /// It looks like the following:
    ///
    /// https://ms-************.sfo.meilisearch.io
    #[structopt(long)]
    url: String,

    /// The index name you want to send your documents in.
    #[structopt(long)]
    index: String,

    /// The name of the field that must be used by Meilisearch to uniquely identify your documents.
    /// If not specified here, Meilisearch will try it's best to guess it.
    #[structopt(long)]
    primary_key: Option<String>,

    /// The API key to access Meilisearch. This API key must have the `documents.add` right.
    /// The Master Key and the Default Admin API Key can be used to send documents.
    #[structopt(long)]
    api_key: Option<String>,

    /// The delimiter to use for the CSV files.
    #[structopt(long, default_value_t = b',')]
    csv_delimiter: u8,

    /// Defines whether we send the embeddings to the remote server or do not send a single embedding.
    #[structopt(long)]
    ignore_embeddings: bool,

    /// A list of file paths that are streamed and sent to Meilisearch in batches,
    /// where content can come from stdin using the special minus (-) path.
    #[structopt(long, num_args(1..))]
    files: Vec<PathBuf>,

    /// The file format to use. Overrides auto-detection, useful for stdin input (-).
    #[structopt(long)]
    format: Option<Mime>,

    /// The size of the batches sent to Meilisearch.
    #[structopt(long, default_value = "20 MiB")]
    batch_size: Byte,

    /// The number of parallel jobs to use when uploading data.
    ///
    /// Be careful to make sure your data can be sent in batches and order of the documents doesn't matter.
    /// Also make sure not to overload the Meilisearch instance with too many jobs.
    #[structopt(long, default_value = "1")]
    jobs: NonZero<usize>,

    /// The number of batches to skip. Useful when the upload stopped for some reason.
    #[structopt(long)]
    skip_batches: Option<u64>,

    /// Tells us to read data from stdin and to use the provided format.
    #[structopt(long, conflicts_with("files"))]
    stdin: Option<Mime>,

    /// The operation to perform when uploading a document.
    #[arg(
        long,
        value_name = "OPERATION",
        num_args = 0..=1,
        default_value_t = DocumentOperation::AddOrReplace,
        value_enum
    )]
    upload_operation: DocumentOperation,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum DocumentOperation {
    AddOrReplace,
    AddOrUpdate,
}

fn send_data(
    opt: &Opt,
    agent: &Agent,
    upload_operation: DocumentOperation,
    pb: &ProgressBar,
    mime: &Mime,
    data: &[u8],
) -> anyhow::Result<()> {
    let api_key = opt.api_key.clone();
    let mut url = format!("{}/indexes/{}/documents", opt.url, opt.index);
    if let Some(primary_key) = &opt.primary_key {
        url = format!("{}?primaryKey={}", url, primary_key);
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    let data = encoder.finish()?;

    let retries = 20;
    let min = Duration::from_millis(100); // 10ms
    let max = Duration::from_secs(60 * 60); // 1h
    let backoff = Backoff::new(retries, min, max);

    for (attempt, duration) in backoff.into_iter().enumerate() {
        let mut request = match upload_operation {
            DocumentOperation::AddOrReplace => agent.post(&url),
            DocumentOperation::AddOrUpdate => agent.put(&url),
        };
        request = request.set("Content-Type", mime.as_str());
        request = request.set("Content-Encoding", "gzip");
        request = request.set("X-Meilisearch-Client", "Meilisearch Importer");

        if let Some(api_key) = &api_key {
            request = request.set("Authorization", &format!("Bearer {}", api_key));
        }

        match request.send_bytes(&data) {
            Ok(response) if matches!(response.status(), 200..=299) => return Ok(()),
            Ok(response) => {
                let e = response.into_string()?;
                pb.println(format!("Attempt #{attempt}: {e}"));
                thread::sleep(duration);
            }
            Err(e) => {
                pb.println(format!("Attempt #{attempt}: {e}"));
                thread::sleep(duration);
            }
        }
    }

    anyhow::bail!("Too many errors. Stopping the retries.")
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    let agent = AgentBuilder::new().timeout(Duration::from_secs(30)).build();
    let files = match opt.stdin {
        Some(_) => vec![PathBuf::from("-")],
        None => opt.files.clone(),
    };

    // for each files present in the argument
    for path in files {
        // check if the file exists
        if path != Path::new("-") && !path.exists() {
            anyhow::bail!("The file {:?} does not exist", path);
        }

        // get the mime type from either the stdin argument, the format argument if provided or from
        // the extension of the file.
        let mime = match opt.stdin {
            Some(mime) => mime,
            None => match opt.format {
                Some(mime) => mime,
                None => Mime::from_path(&path).context("Could not find the mime type")?,
            },
        };

        let pool = ThreadPoolBuilder::new().num_threads(opt.jobs.get()).build()?;

        if opt.ignore_embeddings && mime != Mime::NdJson {
            anyhow::bail!("Ignoring embeddings can only be used with NDJSON files");
        }

        let file_size = if path == Path::new("-") { 0 } else { fs::metadata(&path)?.len() };
        let size = opt.batch_size.as_u64() as usize;
        let nb_chunks = file_size / size as u64;
        let pb = if file_size > 0 {
            let progress_style =
                ProgressStyle::with_template("{wide_bar} {pos}/{len} [{per_sec}] ({eta})").unwrap();
            ProgressBar::new(nb_chunks).with_style(progress_style)
        } else {
            ProgressBar::new_spinner()
        };
        pb.inc(0);

        match mime {
            Mime::Json => {
                if opt.skip_batches.zip(pb.length()).map_or(true, |(s, l)| s > l) {
                    let data = fs::read_to_string(path)?;
                    send_data(&opt, &agent, opt.upload_operation, &pb, &mime, data.as_bytes())?;
                }
                pb.inc(1);
            }
            Mime::NdJson => {
                thread::scope(|s| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(100);
                    let producer_handle = s.spawn(move || {
                        for chunk in nd_json::NdJsonChunker::new(path, size, opt.ignore_embeddings) {
                            tx.send(chunk)?;
                        }
                        Ok(()) as anyhow::Result<()>
                    });

                    let sender_handle =
                        s.spawn(|| send_producer_in_parallel(&opt, &agent, &pb, &pool, &mime, rx));

                    producer_handle.join().unwrap()?;
                    sender_handle.join().unwrap()?;

                    Ok(()) as anyhow::Result<()>
                })?;
            }
            Mime::Csv => {
                thread::scope(|s| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(100);
                    let producer_handle = s.spawn(move || {
                        for chunk in csv::CsvChunker::new(path, size, opt.csv_delimiter) {
                            tx.send(chunk)?;
                        }
                        Ok(()) as anyhow::Result<()>
                    });

                    let sender_handle =
                        s.spawn(|| send_producer_in_parallel(&opt, &agent, &pb, &pool, &mime, rx));

                    producer_handle.join().unwrap()?;
                    sender_handle.join().unwrap()?;

                    Ok(()) as anyhow::Result<()>
                })?;
            }
        }
    }

    Ok(())
}

fn send_producer_in_parallel(
    opt: &Opt,
    agent: &Agent,
    pb: &ProgressBar,
    pool: &ThreadPool,
    mime: &Mime,
    rx: Receiver<Vec<u8>>,
) -> anyhow::Result<()> {
    pool.install(|| {
        rx.into_iter().par_bridge().try_for_each(|chunk| {
            if opt.skip_batches.zip(pb.length()).map_or(true, |(s, l)| s > l) {
                send_data(&opt, &agent, opt.upload_operation, &pb, &mime, &chunk)?;
            }
            pb.inc(1);
            Ok(()) as anyhow::Result<()>
        })
    })
}
