use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

use crate::parser::{
    BodyJson, CDMSpecJson, IterationFKJson, IterationJson, IterationSpecJson, MetricDataJson,
    MetricDataSpecJson, MetricDescFKJson, MetricDescJson, MetricDescSpecJson, PeriodFKJson,
    PeriodJson, PeriodSpecJson, RunFKJson, RunJson, RunSpecJson, SampleFKJson, SampleJson,
    SampleSpecJson, TagJson, TagSpecJson, date_time_utc_from_str, insert_records,
};

#[derive(Error, Debug)]
pub enum AddError {
    #[error("Couldn't find path, or it isn't a directory: {0}")]
    InvalidPath(String),
    #[error("Failed to deserialize {0}: {1}")]
    JSONParseFailed(String, String),
    #[error("Unknown CDM index referenced {0}")]
    UnknownIndex(String),
    #[error("Couldn't parse data point {0}")]
    PointParseFailed(String),
    #[error("Couldn't parse timestamp {0}")]
    TimestampParseFailed(String),
    #[error("Couldn't insert row into CDM table {0}")]
    InsertFailed(String),
}

fn is_json(path: &str) -> bool {
    let length = path.len();
    let extension = path.get(length - 5..length);
    match extension {
        Some(ext) => ext == ".json",
        None => false,
    }
}

pub fn date_time_utc_from_ms_timestamp<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let n: i64 = Deserialize::deserialize(deserializer)?;
    let res = DateTime::from_timestamp_millis(n)
        .ok_or(AddError::TimestampParseFailed(n.to_string()))
        .map_err(de::Error::custom)?;
    Ok(res)
}

pub fn point_from_array<'de, D>(deserializer: D) -> Result<Vec<Point>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Vec<Vec<f64>> = Deserialize::deserialize(deserializer)?;
    let mut points = Vec::new();
    for p in s {
        let new_point = Point {
            begin: DateTime::from_timestamp_millis(
                *p.get(0)
                    .ok_or(AddError::PointParseFailed(format!("{:?}", p)))
                    .map_err(de::Error::custom)? as i64,
            )
            .ok_or(AddError::TimestampParseFailed(format!("{:?}", p)))
            .map_err(de::Error::custom)?,
            finish: DateTime::from_timestamp_millis(
                *p.get(1)
                    .ok_or(AddError::PointParseFailed(format!("{:?}", p)))
                    .map_err(de::Error::custom)? as i64,
            )
            .ok_or(AddError::TimestampParseFailed(format!("{:?}", p)))
            .map_err(de::Error::custom)?,
            value: *p
                .get(2)
                .ok_or(AddError::PointParseFailed(format!("{:?}", p)))
                .map_err(de::Error::custom)?,
        };
        points.push(new_point);
    }
    Ok(points)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RunNode {
    #[serde(default = "Uuid::new_v4", rename = "run-uuid")]
    pub run_uuid: Uuid,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub begin: DateTime<Utc>,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub finish: DateTime<Utc>,
    pub benchmark: String,
    pub email: String,
    pub name: String,
    pub description: Option<String>,
    pub source: String,
    pub tags: HashMap<String, String>,
    pub iterations: Vec<IterationNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TagNode {
    pub name: String,
    pub val: String,
}

fn default_metric() -> String {
    "metric".to_string()
}

fn default_period() -> String {
    "period".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IterationNode {
    #[serde(default = "Uuid::new_v4", rename = "iteration-uuid")]
    pub iteration_uuid: Uuid,
    pub num: i64,
    pub status: String,
    pub path: Option<String>,
    #[serde(default = "default_metric")]
    pub primary_metric: String,
    #[serde(default = "default_period")]
    pub primary_period: String,
    pub params: HashMap<String, String>,
    pub samples: Vec<SampleNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleNode {
    #[serde(default = "Uuid::new_v4", rename = "sample-uuid")]
    pub sample_uuid: Uuid,
    pub num: i64,
    pub status: String,
    pub path: Option<String>,
    pub periods: Vec<PeriodNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeriodNode {
    #[serde(default = "Uuid::new_v4", rename = "period-uuid")]
    pub period_uuid: Uuid,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub begin: DateTime<Utc>,
    #[serde(deserialize_with = "date_time_utc_from_str")]
    pub finish: DateTime<Utc>,
    pub name: String,
    pub metrics: Vec<MetricNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricNode {
    #[serde(default = "Uuid::new_v4", rename = "metric-desc-uuid")]
    pub metric_desc_uuid: Uuid,
    pub class: String,
    #[serde(rename = "metric-type")]
    pub metric_type: String,
    pub source: String,
    pub names: HashMap<String, String>,
    #[serde(deserialize_with = "point_from_array")]
    pub data: Vec<Point>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Point {
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub value: f64,
}

fn run_to_body_jsons(run_node: RunNode) -> Vec<BodyJson> {
    let mut bodies: Vec<BodyJson> = Vec::new();
    let cdm_spec = CDMSpecJson {
        ver: "v8dev".to_string(),
    };

    let run = BodyJson::Run(RunJson {
        cdm: cdm_spec.clone(),
        run: RunSpecJson {
            run_uuid: run_node.run_uuid,
            begin: run_node.begin,
            end: run_node.finish,
            benchmark: run_node.benchmark,
            email: run_node.email,
            name: run_node.name,
            description: run_node.description,
            source: run_node.source,
        },
    });
    bodies.push(run);

    for (name, val) in run_node.tags {
        let tag = BodyJson::Tag(TagJson {
            cdm: cdm_spec.clone(),
            tag: TagSpecJson { name, val },
            run: RunFKJson {
                run_uuid: run_node.run_uuid,
            },
        });
        bodies.push(tag);
    }

    for iteration in run_node.iterations {
        let iteration_json = BodyJson::Iteration(IterationJson {
            cdm: cdm_spec.clone(),
            iteration: IterationSpecJson {
                iteration_uuid: iteration.iteration_uuid,
                num: iteration.num,
                primary_metric: iteration.primary_metric,
                primary_period: iteration.primary_period,
                status: iteration.status,
                path: iteration.path,
            },
            run: RunFKJson {
                run_uuid: run_node.run_uuid,
            },
        });
        bodies.push(iteration_json);

        for sample in iteration.samples {
            let sample_json = BodyJson::Sample(SampleJson {
                cdm: cdm_spec.clone(),
                sample: SampleSpecJson {
                    sample_uuid: sample.sample_uuid,
                    path: sample.path,
                    num: sample.num,
                    status: sample.status,
                },
                iteration: IterationFKJson {
                    iteration_uuid: iteration.iteration_uuid,
                },
                run: RunFKJson {
                    run_uuid: run_node.run_uuid,
                },
            });
            bodies.push(sample_json);

            for period in sample.periods {
                let period_json = BodyJson::Period(PeriodJson {
                    cdm: cdm_spec.clone(),
                    period: PeriodSpecJson {
                        period_uuid: period.period_uuid,
                        begin: period.begin,
                        end: period.finish,
                        name: period.name,
                    },
                    iteration: IterationFKJson {
                        iteration_uuid: iteration.iteration_uuid,
                    },
                    sample: SampleFKJson {
                        sample_uuid: sample.sample_uuid,
                    },
                    run: RunFKJson {
                        run_uuid: run_node.run_uuid,
                    },
                });
                bodies.push(period_json);

                for metric in period.metrics {
                    let metric_desc_json = BodyJson::MetricDesc(MetricDescJson {
                        cdm: cdm_spec.clone(),
                        iteration: Some(IterationFKJson {
                            iteration_uuid: iteration.iteration_uuid,
                        }),
                        run: RunFKJson {
                            run_uuid: run_node.run_uuid,
                        },
                        metric_desc: MetricDescSpecJson {
                            metric_desc_uuid: metric.metric_desc_uuid,
                            class: metric.class,
                            metric_type: metric.metric_type,
                            source: metric.source,
                            names_list: metric.names.keys().cloned().collect(),
                            names: metric
                                .names
                                .iter()
                                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                                .collect(),
                        },
                        period: Some(PeriodFKJson {
                            period_uuid: period.period_uuid,
                        }),
                        sample: Some(SampleFKJson {
                            sample_uuid: sample.sample_uuid,
                        }),
                    });
                    bodies.push(metric_desc_json);

                    for point in metric.data {
                        let metric_data_json = BodyJson::MetricData(MetricDataJson {
                            cdm: cdm_spec.clone(),
                            metric_data: MetricDataSpecJson {
                                begin: point.begin,
                                end: point.finish,
                                duration: (point.finish - point.begin).num_milliseconds(),
                                value: point.value,
                            },
                            metric_desc: MetricDescFKJson {
                                metric_desc_uuid: metric.metric_desc_uuid,
                            },
                            run: RunFKJson {
                                run_uuid: run_node.run_uuid,
                            },
                        });
                        bodies.push(metric_data_json);
                    }
                }
            }
        }
    }

    bodies
}

pub async fn add(pool: &PgPool, path: &Path) -> Result<()> {
    let json_paths: Vec<PathBuf> = match fs::read_dir(path) {
        Ok(files) => {
            let paths = files
                .into_iter()
                .filter(|f| f.is_ok())
                .filter_map(|f| f.ok())
                .map(|d| d.path());

            paths
                .filter(|p| p.to_str().map(is_json).unwrap_or(false))
                .collect()
        }
        Err(_) => {
            vec![PathBuf::from(path)]
        }
    };

    let mut records: Vec<BodyJson> = Vec::new();

    for json_path in json_paths {
        let f = File::open(json_path.clone()).map_err(|_| {
            AddError::InvalidPath(format!(
                "Couldn't open file {}",
                json_path.to_str().unwrap_or("path")
            ))
        })?;

        let run_node: Vec<RunNode> = serde_json::from_reader(f).map_err(|e| {
            AddError::JSONParseFailed(
                json_path.to_str().unwrap_or("path").to_string(),
                e.to_string(),
            )
        })?;
        records.extend(run_node.into_iter().map(run_to_body_jsons).flatten());
    }

    // Ingest the documents in one transaction
    let mut txn = pool.begin().await?;

    let total_records = insert_records(&mut txn, &records).await?;

    txn.commit().await?;

    println!("added {} rows", total_records);

    Ok(())
}
