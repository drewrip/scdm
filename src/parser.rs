use anyhow::Result;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::fs::File;
use std::io::{BufReader, prelude::*};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Couldn't find path, or it isn't a directory: {0}")]
    InvalidPath(String),
    #[error("Failed to deserialize {0}: {1}")]
    JSONParseFailed(String, String),
    #[error("Unknown CDM index referenced {0}")]
    UnknownIndex(String),
    #[error("Couldn't parse timestamp {0}")]
    TimestampParseFailed(String),
}

fn date_time_utc_from_str<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let n: i64 = s.parse().map_err(de::Error::custom)?;
    DateTime::from_timestamp_millis(n)
        .ok_or(ParseError::TimestampParseFailed(format!("{}", n)))
        .map_err(de::Error::custom)
}

fn number_from_str<'de, D, F>(deserializer: D) -> Result<F, D::Error>
where
    D: Deserializer<'de>,
    F: FromStr,
    F::Err: Display,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(de::Error::custom)
}

fn is_ndjson(path: &str) -> bool {
    let length = path.len();
    let extension = path.get(length - 7..length);
    match extension {
        Some(ext) => ext == ".ndjson",
        None => false,
    }
}

fn index_name_to_type(name: String) -> Option<IndexType> {
    match name.split("dev-").nth(1)?.split("@").nth(0)? {
        "iteration" => Some(IndexType::Iteration),
        "metric_data" => Some(IndexType::MetricData),
        "metric_desc" => Some(IndexType::MetricDesc),
        "param" => Some(IndexType::Param),
        "period" => Some(IndexType::Period),
        "run" => Some(IndexType::Run),
        "sample" => Some(IndexType::Sample),
        "tag" => Some(IndexType::Tag),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexJson {
    pub index: IndexSpecJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSpecJson {
    pub _index: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CDMSpecJson {
    pub ver: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IterationJson {
    pub cdm: CDMSpecJson,
    pub iteration: IterationSpecJson,
    pub run: RunFKJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IterationSpecJson {
    #[serde(rename = "iteration-uuid")]
    pub iteration_uuid: Uuid,
    pub num: i64,
    #[serde(rename = "primary-metric")]
    pub primary_metric: String,
    #[serde(rename = "primary-period")]
    pub primary_period: String,
    pub status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IterationFKJson {
    #[serde(rename = "iteration-uuid")]
    pub iteration_uuid: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricDataJson {
    pub cdm: CDMSpecJson,
    pub metric_data: MetricDataSpecJson,
    pub metric_desc: MetricDescFKJson,
    pub run: RunFKJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricDataSpecJson {
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub begin: DateTime<Utc>,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub end: DateTime<Utc>,
    pub duration: u64, // In milliseconds
    #[serde(deserialize_with = "number_from_str")]
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricDescJson {
    pub cdm: CDMSpecJson,
    pub metric_desc: MetricDescSpecJson,
    pub iteration: Option<IterationFKJson>,
    pub period: Option<PeriodFKJson>,
    pub run: RunFKJson,
    pub sample: Option<SampleFKJson>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricDescSpecJson {
    #[serde(rename = "metric_desc-uuid")]
    pub metric_desc_uuid: Uuid,
    pub class: String,
    pub names: HashMap<String, Value>,
    #[serde(rename = "names-list")]
    pub names_list: Vec<String>,
    pub source: String,
    #[serde(rename = "type")]
    pub metric_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricDescFKJson {
    #[serde(rename = "metric_desc-uuid")]
    pub metric_desc_uuid: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParamJson {
    pub cdm: CDMSpecJson,
    pub param: ParamSpecJson,
    pub iteration: IterationFKJson,
    pub run: RunFKJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParamSpecJson {
    pub arg: String,
    pub val: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeriodJson {
    pub cdm: CDMSpecJson,
    pub period: PeriodSpecJson,
    pub iteration: IterationFKJson,
    pub run: RunFKJson,
    pub sample: SampleFKJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeriodSpecJson {
    #[serde(rename = "period-uuid")]
    pub period_uuid: Uuid,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub begin: DateTime<Utc>,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub end: DateTime<Utc>,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeriodFKJson {
    #[serde(rename = "period-uuid")]
    pub period_uuid: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunJson {
    pub cdm: CDMSpecJson,
    pub run: RunSpecJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSpecJson {
    #[serde(rename = "run-uuid")]
    pub run_uuid: Uuid,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub begin: DateTime<Utc>,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub end: DateTime<Utc>,
    pub benchmark: String,
    pub email: String,
    pub name: String,
    pub source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunFKJson {
    #[serde(rename = "run-uuid")]
    pub run_uuid: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleJson {
    pub cdm: CDMSpecJson,
    pub sample: SampleSpecJson,
    pub iteration: IterationFKJson,
    pub run: RunFKJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleSpecJson {
    #[serde(rename = "sample-uuid")]
    pub sample_uuid: Uuid,
    pub path: Option<String>,
    pub status: String,
    #[serde(deserialize_with = "number_from_str")]
    pub num: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleFKJson {
    #[serde(rename = "sample-uuid")]
    pub sample_uuid: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TagJson {
    pub cdm: CDMSpecJson,
    pub tag: TagSpecJson,
    pub run: RunFKJson,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TagSpecJson {
    pub name: String,
    pub val: String,
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

#[derive(Clone, Debug)]
pub enum BodyJson {
    Iteration(IterationJson),
    MetricData(MetricDataJson),
    MetricDesc(MetricDescJson),
    Param(ParamJson),
    Period(PeriodJson),
    Run(RunJson),
    Sample(SampleJson),
    Tag(TagJson),
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
            let index_type = index_name_to_type(index.index._index.clone())
                .ok_or(ParseError::UnknownIndex(index.index._index))?;

            let body = match index_type {
                IndexType::Iteration => {
                    BodyJson::Iteration(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::MetricData => {
                    BodyJson::MetricData(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::MetricDesc => {
                    BodyJson::MetricDesc(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::Param => {
                    BodyJson::Param(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::Period => {
                    BodyJson::Period(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::Run => {
                    BodyJson::Run(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::Sample => {
                    BodyJson::Sample(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
                IndexType::Tag => {
                    BodyJson::Tag(serde_json::from_str(&body_jsonl).map_err(|e| {
                        ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
                    })?)
                }
            };

            println!("body: {:?}", body);
        }
    }

    // Ingest the documents in one transaction
    let mut txn = pool.begin().await?;
    let res = sqlx::query!("SELECT (1) as c").fetch_all(&mut *txn).await?;

    txn.commit().await?;
    println!("{:?}", res);
    Ok(())
}
