use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use sqlx::{Execute, PgPool, Postgres, QueryBuilder, Transaction};
use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::fs::File;
use std::io::{BufReader, prelude::*};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

use crate::cdm::Name;

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
    #[error("Couldn't insert row into CDM table {0}")]
    InsertFailed(String),
}

#[derive(Debug, Clone)]
pub struct GlobalResource {
    pub iteration: IterationJson,
    pub sample: SampleJson,
    pub period: PeriodJson,
    pub metric_desc: MetricDescJson,
    pub metric_data: MetricDataJson,
}

pub fn date_time_utc_from_str<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    if let Ok(human_readable) = s.parse::<DateTime<Utc>>() {
        Ok(human_readable)
    } else {
        let n: i64 = s
            .parse()
            .map_err(|_| ParseError::TimestampParseFailed(s.to_string()))
            .map_err(de::Error::custom)?;

        let res = DateTime::from_timestamp_millis(n)
            .ok_or(ParseError::TimestampParseFailed(s.to_string()))
            .map_err(de::Error::custom)?;
        Ok(res)
    }
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

pub trait Global {
    fn global(parent_uuid: Uuid, my_uuid: Uuid) -> Self;
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
    pub path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IterationFKJson {
    #[serde(rename = "iteration-uuid")]
    pub iteration_uuid: Uuid,
}

impl Global for IterationJson {
    fn global(parent_uuid: Uuid, my_uuid: Uuid) -> Self {
        IterationJson {
            cdm: CDMSpecJson {
                ver: "v8dev".to_string(),
            },
            iteration: IterationSpecJson {
                iteration_uuid: my_uuid,
                num: 0,
                primary_metric: "global".to_string(),
                primary_period: "global".to_string(),
                status: "pass".to_string(),
                path: None,
            },
            run: RunFKJson {
                run_uuid: parent_uuid,
            },
        }
    }
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
    pub duration: i64, // In milliseconds
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

impl Global for MetricDataJson {
    fn global(parent_uuid: Uuid, _my_uuid: Uuid) -> Self {
        MetricDataJson {
            cdm: CDMSpecJson {
                ver: "v8dev".to_string(),
            },
            metric_data: MetricDataSpecJson {
                begin: DateTime::<Utc>::from_timestamp_nanos(0),
                end: DateTime::<Utc>::from_timestamp_nanos(0),
                duration: 0,
                value: 0.0,
            },
            metric_desc: MetricDescFKJson {
                metric_desc_uuid: parent_uuid,
            },
            run: RunFKJson {
                run_uuid: Uuid::nil(),
            },
        }
    }
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

impl Global for MetricDescJson {
    fn global(parent_uuid: Uuid, my_uuid: Uuid) -> Self {
        MetricDescJson {
            cdm: CDMSpecJson {
                ver: "v8dev".to_string(),
            },
            metric_desc: MetricDescSpecJson {
                metric_desc_uuid: my_uuid,
                class: "count".to_string(),
                names: HashMap::new(),
                names_list: Vec::new(),
                source: "global".to_string(),
                metric_type: "global".to_string(),
            },
            iteration: None,
            period: Some(PeriodFKJson {
                period_uuid: parent_uuid,
            }),
            run: RunFKJson {
                run_uuid: Uuid::nil(),
            },
            sample: None,
        }
    }
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

impl Global for PeriodJson {
    fn global(parent_uuid: Uuid, my_uuid: Uuid) -> Self {
        PeriodJson {
            cdm: CDMSpecJson {
                ver: "v8dev".to_string(),
            },
            period: PeriodSpecJson {
                period_uuid: my_uuid,
                begin: DateTime::<Utc>::from_timestamp_nanos(0),
                end: DateTime::<Utc>::from_timestamp_nanos(0),
                name: "global".to_string(),
            },
            iteration: IterationFKJson {
                iteration_uuid: Uuid::nil(),
            },
            run: RunFKJson {
                run_uuid: Uuid::nil(),
            },
            sample: SampleFKJson {
                sample_uuid: parent_uuid,
            },
        }
    }
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
    pub description: Option<String>,
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

impl Global for SampleJson {
    fn global(parent_uuid: Uuid, my_uuid: Uuid) -> Self {
        SampleJson {
            cdm: CDMSpecJson {
                ver: "v8dev".to_string(),
            },
            sample: SampleSpecJson {
                sample_uuid: my_uuid,
                path: None,
                status: "pass".to_string(),
                num: 0,
            },
            iteration: IterationFKJson {
                iteration_uuid: parent_uuid,
            },
            run: RunFKJson {
                run_uuid: Uuid::nil(), // Doesn't matter for our processing, not the direct parent
            },
        }
    }
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
    Name(Name),
}

fn parse_body(index_type: IndexType, body_jsonl: String) -> Result<BodyJson> {
    Ok(match index_type {
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
        IndexType::Param => BodyJson::Param(serde_json::from_str(&body_jsonl).map_err(|e| {
            ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
        })?),
        IndexType::Period => BodyJson::Period(serde_json::from_str(&body_jsonl).map_err(|e| {
            ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
        })?),
        IndexType::Run => BodyJson::Run(serde_json::from_str(&body_jsonl).map_err(|e| {
            ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
        })?),
        IndexType::Sample => BodyJson::Sample(serde_json::from_str(&body_jsonl).map_err(|e| {
            ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
        })?),
        IndexType::Tag => BodyJson::Tag(serde_json::from_str(&body_jsonl).map_err(|e| {
            ParseError::JSONParseFailed(format!("{:?}", index_type), e.to_string())
        })?),
    })
}

pub async fn insert_runs(
    txn: &mut Transaction<'_, Postgres>,
    globals: &mut HashMap<Uuid, GlobalResource>,
    runs: &Vec<&RunJson>,
) -> Result<(
    u64,
    Vec<IterationJson>,
    Vec<SampleJson>,
    Vec<PeriodJson>,
    Vec<MetricDescJson>,
    Vec<MetricDataJson>,
)> {
    let mut global_iterations = Vec::new();
    let mut global_samples = Vec::new();
    let mut global_periods = Vec::new();
    let mut global_metric_descs = Vec::new();
    let mut global_metric_datas = Vec::new();
    if runs.is_empty() {
        return Ok((
            0,
            global_iterations,
            global_samples,
            global_periods,
            global_metric_descs,
            global_metric_datas,
        ));
    }

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO run
        (run_uuid, begin, finish, benchmark, email, name, description, source) ",
    );
    qb.push_values(runs, |mut b, run| {
        let iteration_uuid = Uuid::new_v4();
        let global_iteration = IterationJson::global(run.run.run_uuid, iteration_uuid);
        let sample_uuid = Uuid::new_v4();
        let global_sample = SampleJson::global(iteration_uuid, sample_uuid);
        let period_uuid = Uuid::new_v4();
        let global_period = PeriodJson::global(sample_uuid, period_uuid);
        let metric_desc_uuid = Uuid::new_v4();
        let global_metric_desc = MetricDescJson::global(period_uuid, metric_desc_uuid);
        let global_metric_data = MetricDataJson::global(metric_desc_uuid, Uuid::nil());
        global_iterations.push(global_iteration.clone());
        global_samples.push(global_sample.clone());
        global_periods.push(global_period.clone());
        global_metric_descs.push(global_metric_desc.clone());
        global_metric_datas.push(global_metric_data.clone());
        let global_resource = GlobalResource {
            iteration: global_iteration,
            sample: global_sample,
            period: global_period,
            metric_desc: global_metric_desc,
            metric_data: global_metric_data,
        };
        globals.insert(run.run.run_uuid, global_resource);
        b.push_bind(run.run.run_uuid)
            .push_bind(run.run.begin)
            .push_bind(run.run.end)
            .push_bind(&run.run.benchmark)
            .push_bind(&run.run.email)
            .push_bind(&run.run.name)
            .push_bind(&run.run.description)
            .push_bind(&run.run.source);
    });
    let query = qb.build();
    let s = query.sql();
    let res = query
        .execute(&mut **txn)
        .await
        .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
    Ok((
        res.rows_affected(),
        global_iterations,
        global_samples,
        global_periods,
        global_metric_descs,
        global_metric_datas,
    ))
}

pub async fn insert_tags(txn: &mut Transaction<'_, Postgres>, tags: &Vec<&TagJson>) -> Result<u64> {
    if tags.is_empty() {
        return Ok(0);
    }

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO tag
        (run_uuid, name, val) ",
    );
    qb.push_values(tags, |mut b, tag| {
        b.push_bind(tag.run.run_uuid)
            .push_bind(&tag.tag.name)
            .push_bind(&tag.tag.val);
    });
    let query = qb.build();
    let s = query.sql();
    let res = query
        .execute(&mut **txn)
        .await
        .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
    Ok(res.rows_affected())
}

pub async fn insert_iterations(
    txn: &mut Transaction<'_, Postgres>,
    iterations: &Vec<&IterationJson>,
) -> Result<u64> {
    if iterations.is_empty() {
        return Ok(0);
    }

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO iteration
        (iteration_uuid, run_uuid, num, status, path, primary_metric, primary_period) ",
    );
    qb.push_values(iterations, |mut b, iteration| {
        b.push_bind(iteration.iteration.iteration_uuid)
            .push_bind(iteration.run.run_uuid)
            .push_bind(iteration.iteration.num)
            .push_bind(&iteration.iteration.status)
            .push_bind(&iteration.iteration.path)
            .push_bind(&iteration.iteration.primary_metric)
            .push_bind(&iteration.iteration.primary_period);
    });
    let query = qb.build();
    let s = query.sql();
    let res = query
        .execute(&mut **txn)
        .await
        .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
    Ok(res.rows_affected())
}

pub async fn insert_params(
    txn: &mut Transaction<'_, Postgres>,
    params: &Vec<&ParamJson>,
) -> Result<u64> {
    if params.is_empty() {
        return Ok(0);
    }

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO param
        (iteration_uuid, arg, val) ",
    );
    qb.push_values(params, |mut b, param| {
        b.push_bind(param.iteration.iteration_uuid)
            .push_bind(&param.param.arg)
            .push_bind(&param.param.val);
    });
    let query = qb.build();
    let s = query.sql();
    let res = query
        .execute(&mut **txn)
        .await
        .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
    Ok(res.rows_affected())
}

pub async fn insert_samples(
    txn: &mut Transaction<'_, Postgres>,
    samples: &Vec<&SampleJson>,
) -> Result<u64> {
    if samples.is_empty() {
        return Ok(0);
    }

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO sample
        (sample_uuid, iteration_uuid, num, status, path) ",
    );
    qb.push_values(samples, |mut b, sample| {
        b.push_bind(sample.sample.sample_uuid)
            .push_bind(&sample.iteration.iteration_uuid)
            .push_bind(sample.sample.num)
            .push_bind(&sample.sample.status)
            .push_bind(&sample.sample.path);
    });
    let query = qb.build();
    let s = query.sql();
    let res = query
        .execute(&mut **txn)
        .await
        .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
    Ok(res.rows_affected())
}

pub async fn insert_periods(
    txn: &mut Transaction<'_, Postgres>,
    periods: &Vec<&PeriodJson>,
) -> Result<u64> {
    if periods.is_empty() {
        return Ok(0);
    }

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO period
        (period_uuid, sample_uuid, begin, finish, name) ",
    );
    qb.push_values(periods, |mut b, period| {
        b.push_bind(period.period.period_uuid)
            .push_bind(period.sample.sample_uuid)
            .push_bind(period.period.begin)
            .push_bind(period.period.end)
            .push_bind(&period.period.name);
    });
    let query = qb.build();
    let s = query.sql();
    let res = query
        .execute(&mut **txn)
        .await
        .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
    Ok(res.rows_affected())
}

pub async fn insert_metric_descs(
    txn: &mut Transaction<'_, Postgres>,
    globals: &HashMap<Uuid, GlobalResource>,
    metric_descs: &Vec<&MetricDescJson>,
) -> Result<u64> {
    if metric_descs.is_empty() {
        return Ok(0);
    }

    let mut rows_affected = 0;
    for group in metric_descs.chunks(1024) {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO metric_desc
        (metric_desc_uuid, period_uuid, class, metric_type, source, names_list, names) ",
        );
        qb.push_values(group, |mut b, metric_desc| {
            b.push_bind(metric_desc.metric_desc.metric_desc_uuid)
                .push_bind(
                    metric_desc
                        .period
                        .clone()
                        .map(|p| p.period_uuid)
                        .or_else(|| {
                            globals
                                .get(&metric_desc.run.run_uuid)
                                .map(|r| r.period.period.period_uuid)
                        }),
                )
                .push_bind(&metric_desc.metric_desc.class)
                .push_bind(&metric_desc.metric_desc.metric_type)
                .push_bind(&metric_desc.metric_desc.source)
                .push_bind(&metric_desc.metric_desc.names_list)
                .push_bind(serde_json::to_string(&metric_desc.metric_desc.names).ok());
        });
        let query = qb.build();
        let s = query.sql();
        let res = query
            .execute(&mut **txn)
            .await
            .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
        rows_affected += res.rows_affected();
    }
    Ok(rows_affected)
}

pub async fn insert_names(txn: &mut Transaction<'_, Postgres>, names: &Vec<&Name>) -> Result<u64> {
    if names.is_empty() {
        return Ok(0);
    }

    let mut rows_affected = 0;
    for group in names.chunks(1024) {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO name
        (metric_desc_uuid, name, val) ",
        );
        qb.push_values(group, |mut b, name| {
            b.push_bind(name.metric_desc_uuid)
                .push_bind(&name.name)
                .push_bind(&name.val);
        });
        let query = qb.build();
        let s = query.sql();
        let res = query
            .execute(&mut **txn)
            .await
            .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
        rows_affected += res.rows_affected();
    }
    Ok(rows_affected)
}

pub async fn insert_metric_datas(
    txn: &mut Transaction<'_, Postgres>,
    metric_datas: &Vec<&MetricDataJson>,
) -> Result<u64> {
    if metric_datas.is_empty() {
        return Ok(0);
    }
    let mut rows_affected = 0;
    for group in metric_datas.chunks(1024) {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO metric_data
        (metric_desc_uuid, value, begin, finish, duration) ",
        );
        qb.push_values(group, |mut b, metric_data| {
            b.push_bind(metric_data.metric_desc.metric_desc_uuid)
                .push_bind(metric_data.metric_data.value)
                .push_bind(metric_data.metric_data.begin)
                .push_bind(metric_data.metric_data.end)
                .push_bind(metric_data.metric_data.duration);
        });
        let query = qb.build();
        let s = query.sql();
        let res = query
            .execute(&mut **txn)
            .await
            .map_err(|e| ParseError::InsertFailed(format!("{} ({})", e.to_string(), s)))?;
        rows_affected += res.rows_affected();
    }
    Ok(rows_affected)
}

pub fn extract_names(metric_desc: &MetricDescJson) -> Vec<Name> {
    metric_desc
        .metric_desc
        .names
        .iter()
        .filter_map(|md| match md.1.as_str() {
            Some(val) => Some(Name {
                metric_desc_uuid: metric_desc.metric_desc.metric_desc_uuid,
                name: md.0.to_string(),
                val: val.to_string(),
            }),
            None => None,
        })
        .collect()
}

pub async fn insert_records(
    txn: &mut Transaction<'_, Postgres>,
    records: &Vec<BodyJson>,
) -> Result<u64> {
    let mut num_new = 0;
    let mut runs = Vec::new();
    let mut tags = Vec::new();
    let mut iterations = Vec::new();
    let mut params = Vec::new();
    let mut samples = Vec::new();
    let mut periods = Vec::new();
    let mut metric_descs = Vec::new();
    let mut metric_datas = Vec::new();
    let mut names = Vec::new();

    for record in records {
        match record {
            BodyJson::Run(run) => runs.push(run),
            BodyJson::Tag(tag) => tags.push(tag),
            BodyJson::Iteration(iteration) => iterations.push(iteration),
            BodyJson::Param(param) => params.push(param),
            BodyJson::Sample(sample) => samples.push(sample),
            BodyJson::Period(period) => periods.push(period),
            BodyJson::MetricDesc(metric_desc) => metric_descs.push(metric_desc),
            BodyJson::MetricData(metric_data) => metric_datas.push(metric_data),
            BodyJson::Name(name) => names.push(name.clone()),
        };
    }

    let extracted_names = metric_descs
        .clone()
        .into_iter()
        .map(extract_names)
        .flatten();

    names.extend(extracted_names);

    // Default resources for data that is scoped to the run
    let mut globals: HashMap<Uuid, GlobalResource> = HashMap::new();

    let (
        new_run_rows,
        global_iterations,
        global_samples,
        global_periods,
        global_metric_descs,
        global_metric_datas,
    ) = insert_runs(txn, &mut globals, &runs).await?;
    iterations.append(&mut global_iterations.iter().collect());
    samples.append(&mut global_samples.iter().collect());
    periods.append(&mut global_periods.iter().collect());
    metric_descs.append(&mut global_metric_descs.iter().collect());
    metric_datas.append(&mut global_metric_datas.iter().collect());
    num_new += new_run_rows;

    num_new += insert_tags(txn, &tags).await?;
    num_new += insert_iterations(txn, &iterations).await?;
    num_new += insert_params(txn, &params).await?;
    num_new += insert_samples(txn, &samples).await?;
    num_new += insert_periods(txn, &periods).await?;
    num_new += insert_metric_descs(txn, &globals, &metric_descs).await?;
    num_new += insert_names(txn, &names.iter().collect()).await?;
    num_new += insert_metric_datas(txn, &metric_datas).await?;
    Ok(num_new)
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

    let mut records: Vec<BodyJson> = Vec::new();

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

            records.push(parse_body(index_type, body_jsonl)?);
        }
    }
    // Ingest the documents in one transaction
    let mut txn = pool.begin().await?;

    let total_records = insert_records(&mut txn, &records).await?;

    txn.commit().await?;

    println!("added {} rows", total_records);

    Ok(())
}
