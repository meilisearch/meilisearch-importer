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

    /// Uses port and port+1 to communicate with two instances and
    /// use the meilitool output-formatted-entries command to detect
    /// divergences in between instances.
    ///
    /// It stops at the first divergence, shows the diff, the index
    /// of the document that fails, and the ID of the task.
    ///
    /// It is incompatible with --jobs as sending tasks must be determinist
    /// and sending tasks will be synchronous, waiting for each task to be
    /// accepted and processed on both sides to perform the diff
    /// (search results and key-value content).
    ///
    /// Note that the search queries to perform on both instances will be read,
    /// line by line, from the queries.txt file.
    #[structopt(long, conflicts_with("jobs"))]
    detect_divergences: bool,

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
) -> anyhow::Result<u32> {
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
            Ok(response) if matches!(response.status(), 200..=299) => {
                #[derive(Debug, serde::Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Task {
                    task_uid: u32,
                }

                let Task { task_uid } = response.into_json()?;
                return Ok(task_uid);
            }
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

        // get the mime type from either the stdin argument, the format
        // argument if provided or from the extension of the file.
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
            let progress_style =
                ProgressStyle::with_template("{pos}/??? [{per_sec}] ({elapsed})").unwrap();
            ProgressBar::new_spinner().with_style(progress_style)
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
                        for chunk in nd_json::NdJsonChunker::new(path, size, opt.ignore_embeddings)
                        {
                            tx.send(chunk).context("while sending chunks")?;
                        }
                        Ok(()) as anyhow::Result<()>
                    });

                    let sender_handle =
                        s.spawn(|| send_producer_in_parallel(&opt, &agent, &pb, &pool, &mime, rx));

                    sender_handle.join().unwrap()?;
                    producer_handle.join().unwrap()?;

                    Ok(()) as anyhow::Result<()>
                })?;
            }
            Mime::Csv => {
                thread::scope(|s| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(100);
                    let producer_handle = s.spawn(move || {
                        for chunk in csv::CsvChunker::new(path, size, opt.csv_delimiter) {
                            tx.send(chunk).context("while sending chunks")?;
                        }
                        Ok(()) as anyhow::Result<()>
                    });

                    let sender_handle =
                        s.spawn(|| send_producer_in_parallel(&opt, &agent, &pb, &pool, &mime, rx));

                    sender_handle.join().unwrap()?;
                    producer_handle.join().unwrap()?;

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
    let second_options = if opt.detect_divergences {
        // TODO do better and increment the original port or error
        let url = String::from("http://localhost:7701");
        let opt = Opt { url, ..opt.clone() };
        let queries_string = std::fs::read_to_string("queries.txt").context(
            "opening the queries.txt file - this file is required when using --detect-divergences. \
             Create a queries.txt file with one search query per line in the current directory."
        )?;
        Some((opt, queries_string))
    } else {
        None
    };

    pool.install(|| {
        if let Some((second_opt, queries)) = second_options {
            for chunk in rx {
                if opt.skip_batches.zip(pb.length()).map_or(true, |(s, l)| s > l) {
                    let first_task_uid =
                        send_data(&opt, &agent, opt.upload_operation, &pb, &mime, &chunk)?;
                    let second_task_uid =
                        send_data(&second_opt, &agent, opt.upload_operation, &pb, &mime, &chunk)?;

                    // Only wait once both machines received the tasks
                    wait_for_task(&opt, &agent, first_task_uid)?;
                    wait_for_task(&second_opt, &agent, second_task_uid)?;

                    let mut has_search_divergence = false;
                    let mut has_database_divergence = false;

                    for query in queries.lines().map(|s| s.trim()) {
                        let first_hits = search_instance(opt, agent, query).with_context(|| {
                            format!("searching first instance with query `{query}`")
                        })?;
                        let second_hits =
                            search_instance(&second_opt, agent, query).with_context(|| {
                                format!("searching second instance with query `{query}`")
                            })?;

                        if first_hits != second_hits {
                            use pretty_assertions::Comparison;
                            println!("=== SEARCH RESULTS DIVERGENCE for query '{}' ===", query);
                            println!("{}", Comparison::new(&first_hits, &second_hits));
                            has_search_divergence = true;
                        }
                    }

                    // Diff both databases by piping meilitool outputs directly to diff
                    if let Err(e) = diff_databases(opt, "data.ms", "data1.ms") {
                        println!("=== DATABASE CONTENT DIVERGENCE ===");
                        println!("{:?}", e);
                        has_database_divergence = true;
                    }

                    if has_search_divergence || has_database_divergence {
                        let mut error_parts = Vec::new();
                        if has_search_divergence {
                            error_parts.push("search results");
                        }
                        if has_database_divergence {
                            error_parts.push("database content");
                        }
                        anyhow::bail!("Divergence detected in: {}", error_parts.join(" and "));
                    }
                }
                pb.inc(1);
            }
            Ok(())
        } else {
            rx.into_iter().par_bridge().try_for_each(|chunk| {
                if opt.skip_batches.zip(pb.length()).map_or(true, |(s, l)| s > l) {
                    send_data(&opt, &agent, opt.upload_operation, &pb, &mime, &chunk)?;
                }
                pb.inc(1);
                Ok(())
            })
        }
    })
}

/// Diffs two databases and indexes.
fn diff_databases(
    first_opt: &Opt,
    first_db_path: &str,
    second_db_path: &str,
) -> anyhow::Result<()> {
    use std::process::{Command, Stdio};

    let first_meilitool = Command::new("meilitool")
        .args(["--db-path", first_db_path])
        .arg("compare-entries")
        .args(["--other-db-path", second_db_path])
        .args(["--index-name", &first_opt.index])
        .stderr(Stdio::piped())
        .stdout(Stdio::inherit())
        .spawn()
        .context("Failed to spawn meilitool command")?;

    let output =
        first_meilitool.wait_with_output().context("Failed to wait for meilitool command")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "meilitool command failed with exit code {:?}\nstderr: {}",
            output.status.code(),
            stderr
        );
    }

    Ok(())
}

fn search_instance(
    opt: &Opt,
    agent: &Agent,
    query: &str,
) -> anyhow::Result<Vec<serde_json::Value>> {
    #[derive(Debug, serde::Deserialize)]
    struct SearchResponse {
        hits: Vec<serde_json::Value>,
    }

    let search_payload = serde_json::json!({
        "q": query,
        "limit": 100,
    });

    let url = format!("{}/indexes/{}/search", opt.url, opt.index);
    let mut request = agent.post(&url);
    request = request.set("Content-Type", "application/json");
    request = request.set("X-Meilisearch-Client", "Meilisearch Importer");
    if let Some(api_key) = &opt.api_key {
        request = request.set("Authorization", &format!("Bearer {}", api_key));
    }

    let response: SearchResponse = request.send_json(&search_payload)?.into_json()?;
    Ok(response.hits)
}

fn wait_for_task(opt: &Opt, agent: &Agent, task_uid: u32) -> anyhow::Result<()> {
    let url = format!("{}/tasks/{}", opt.url, task_uid);
    let api_key = opt.api_key.clone();

    loop {
        let mut request = agent.get(&url);
        request = request.set("X-Meilisearch-Client", "Meilisearch Importer");

        if let Some(api_key) = &api_key {
            request = request.set("Authorization", &format!("Bearer {}", api_key));
        }

        let response = request.call()?;

        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct TaskInfo {
            status: String,
            error: Option<serde_json::Value>,
        }

        let task_info: TaskInfo = response.into_json()?;

        match task_info.status.as_str() {
            "enqueued" | "processing" => thread::sleep(Duration::from_millis(100)),
            "succeeded" => return Ok(()),
            "failed" => {
                // Task failed, return the error
                let error_msg = task_info
                    .error
                    .map(|e| format!("Task failed: {}", e))
                    .unwrap_or_else(|| "Task failed with unknown error".to_string());
                anyhow::bail!(error_msg);
            }
            other => {
                anyhow::bail!("Unexpected task status: {}", other)
            }
        }
    }
}
