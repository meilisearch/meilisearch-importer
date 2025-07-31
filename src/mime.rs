use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mime {
    Json,
    NdJson,
    Csv,
}

impl Mime {
    pub fn from_path(path: &Path) -> Option<Mime> {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => Some(Mime::Json),
            Some("ndjson" | "jsonl") => Some(Mime::NdJson),
            Some("csv") => Some(Mime::Csv),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Mime::Json => "application/json",
            Mime::NdJson => "application/x-ndjson",
            Mime::Csv => "text/csv",
        }
    }
}

impl FromStr for Mime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Mime::Json),
            "ndjson" | "jsonl" => Ok(Mime::NdJson),
            "csv" => Ok(Mime::Csv),
            otherwise => anyhow::bail!(
                "unknown {otherwise} file format. Possible values are json, ndjson, jsonl, and csv."
            ),
        }
    }
}
