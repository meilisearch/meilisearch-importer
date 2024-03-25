use std::io::prelude::*;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, thread};

use anyhow::Context;
use byte_unit::Byte;
use clap::Parser;
use exponential_backoff::Backoff;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use mime::Mime;
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

    /// A list of file paths that are streamed and sent to Meilisearch in batches.
    #[structopt(long)]
    files: Vec<PathBuf>,

    /// The size of the batches sent to Meilisearch.
    #[structopt(long, default_value = "20 MiB")]
    batch_size: Byte,
}

fn send_data(
    opt: &Opt,
    agent: &Agent,
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
        let mut request = agent.post(&url);
        request = request.set("Content-Type", mime.as_str());
        request = request.set("Content-Encoding", "gzip");

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
    let files = opt.files.clone();

    // for each files present in the argument
    for file in files {
        // check if the file exists
        if !file.exists() {
            anyhow::bail!("The file {:?} does not exist", file);
        }

        let mime = Mime::from_path(&file).context("Could not find the mime type")?;
        let file_size = fs::metadata(&file)?.len();
        let size = opt.batch_size.as_u64() as usize;
        let nb_chunks = file_size / size as u64;
        let pb = ProgressBar::new(nb_chunks);
        pb.inc(0);

        match mime {
            Mime::Json => {
                let data = fs::read_to_string(file)?;
                send_data(&opt, &agent, &pb, &mime, data.as_bytes())?;
                pb.inc(1);
            }
            Mime::NdJson => {
                for chunk in nd_json::NdJsonChunker::new(file, size) {
                    send_data(&opt, &agent, &pb, &mime, &chunk)?;
                    pb.inc(1);
                }
            }
            Mime::Csv => {
                for chunk in csv::CsvChunker::new(file, size) {
                    send_data(&opt, &agent, &pb, &mime, &chunk)?;
                    pb.inc(1);
                }
            }
        }
    }

    Ok(())
}
