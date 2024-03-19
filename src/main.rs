use std::io::prelude::*;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, thread};

use anyhow::Context;
use byte_unit::Byte;
use exponential_backoff::Backoff;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use mime::Mime;
use structopt::StructOpt;
use ureq::{Agent, AgentBuilder};

mod byte_count;
mod csv;
mod mime;
mod nd_json;

/// An application that chunks the file content to send them to Meilisearch.
#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "importer")]
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
    #[structopt(long, default_value = "90 MB")]
    batch_size: Byte,
}

fn send_data(
    opt: &Opt,
    agent: &Agent,
    pb: &ProgressBar,
    mime: &Mime,
    data: &[u8],
) -> anyhow::Result<()> {
    let token = opt.token.clone();
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
        let result = agent
            .post(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Content-Type", mime.as_str())
            .set("Content-Encoding", "gzip")
            .send_bytes(&data);

        match result {
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
    let opt = Opt::from_args();
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
