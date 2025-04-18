use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::fs;
use std::fs::File;
use std::io::{BufReader, prelude::*};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Couldn't find path, or it isn't a directory: {0}")]
    InvalidPath(String),
    #[error("Failed to deserialize {0}: {1}")]
    JSONParseFailed(String, String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexJson {
    pub index: IndexSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSpec {
    pub _index: String,
}

#[derive(Clone, Debug)]
pub enum IndexType {
    Iteration,
    MetricData,
    MetricDesc,
    Param,
    Period,
    Run,
    Sample,
    Tag,
}

fn is_ndjson(path: &str) -> bool {
    let length = path.len();
    let extension = path.get(length - 7..length);
    match extension {
        Some(ext) => ext == ".ndjson",
        None => false,
    }
}

pub async fn parse(pool: &PgPool, dir_path: &Path) -> Result<()> {
    // Read all of the ndjson files
    let files = fs::read_dir(dir_path).map_err(|_| {
        ParseError::InvalidPath(
            dir_path
                .to_str()
                .map(|s| s.to_string())
                .unwrap_or(format!("{:?}", dir_path)),
        )
    })?;

    let paths = files
        .into_iter()
        .filter(|f| f.is_ok())
        .filter_map(|f| f.ok())
        .map(|d| d.path());

    let ndjson_paths: Vec<PathBuf> = paths
        .filter(|p| p.to_str().map(is_ndjson).unwrap_or(false))
        .collect();

    for ndjson_path in ndjson_paths {
        let f = File::open(ndjson_path.clone()).map_err(|_| {
            ParseError::InvalidPath(format!(
                "Couldn't open file {}",
                ndjson_path.to_str().unwrap_or("path")
            ))
        })?;

        let reader = BufReader::new(f);
        let mut lines = reader.lines();
        while let (Some(Ok(index_jsonl)), Some(Ok(body_jsonl))) = (lines.next(), lines.next()) {
            let index: IndexJson = serde_json::from_str(&index_jsonl)
                .map_err(|e| ParseError::JSONParseFailed("IndexJSON".to_string(), e.to_string()))?;
            println!("Index: {:?}", index);
            println!("Body: {}", body_jsonl);
        }
    }

    // Ingest the documents in one transaction
    let mut txn = pool.begin().await?;
    let res = sqlx::query!("SELECT (1) as c").fetch_all(&mut *txn).await?;

    txn.commit().await?;
    println!("{:?}", res);
    Ok(())
}
