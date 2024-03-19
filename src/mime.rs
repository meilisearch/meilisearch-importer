use std::path::Path;

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
