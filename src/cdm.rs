use chrono::{DateTime, TimeDelta, Utc};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum Class {
    Count,
    Throughput,
}

pub const SQL_TABLE_ITERATION: &str = r#"
    CREATE TABLE IF NOT EXISTS iteration (
        iteration_uuid uuid PRIMARY KEY,
        num bigint NOT NULL,
        status text,
        path text,
        primary_metric text,
        primary_period text
    )
"#;

#[derive(Clone, Debug)]
pub struct Iteration {
    pub iteration_uuid: Uuid,
    pub num: i64,
    pub status: Option<String>,
    pub path: Option<String>,
    pub primary_metric: Option<String>,
    pub primary_period: Option<String>,
}

pub const SQL_TABLE_METRIC_DATA: &str = r#"
    CREATE TABLE IF NOT EXISTS metric_data (
        metric_data_id bigserial PRIMARY KEY,
        metric_desc_uuid uuid,
        value double precision,
        begin timestamp,
        finish timestamp,
        duration bigint,
        CONSTRAINT fk_metric_desc FOREIGN KEY (metric_desc_uuid) REFERENCES metric_desc(metric_desc_uuid)
    )
"#;

#[derive(Clone, Debug)]
pub struct MetricData {
    pub metric_data_id: u64,
    pub metric_desc_uuid: Uuid,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub duration: TimeDelta,
    pub value: f64,
}

pub const SQL_TABLE_METRIC_DESC: &str = r#"
    CREATE TABLE IF NOT EXISTS metric_desc (
        metric_desc_uuid uuid PRIMARY KEY,
        class text,
        type text,
        source text,
        names_list text,
        names text
    )
"#;

#[derive(Clone, Debug)]
pub struct MetricDesc {
    pub metric_desc_uuid: Uuid,
    pub class: Class,
    pub metric_type: String,
    pub source: String,
    pub names_list: Vec<String>,
    pub names: Vec<(String, String)>,
}

pub const SQL_TABLE_PARAM: &str = r#"
    CREATE TABLE IF NOT EXISTS param (
        iteration_uuid uuid,
        arg text,
        val text,
        PRIMARY KEY(iteration_uuid, arg, val),
        CONSTRAINT fk_iteration FOREIGN KEY (iteration_uuid) REFERENCES iteration(iteration_uuid)
    )
"#;

#[derive(Clone, Debug)]
pub struct Param {
    pub iteration_uuid: Uuid,
    pub arg: String,
    pub val: String,
}

pub const SQL_TABLE_PERIOD: &str = r#"
    CREATE TABLE IF NOT EXISTS period (
        period_uuid uuid PRIMARY KEY,
        begin timestamp,
        finish timestamp,
        name text,
        prev_id uuid
    )
"#;

#[derive(Clone, Debug)]
pub struct Period {
    pub period_uuid: Uuid,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub name: String,
}

pub const SQL_TABLE_RUN: &str = r#"
    CREATE TABLE IF NOT EXISTS run (
        run_uuid uuid PRIMARY KEY,
        begin timestamp,
        finish timestamp,
        benchmark text,
        email text,
        name text,
        description text,
        tags text,
        source text
    )
"#;

#[derive(Clone, Debug)]
pub struct Run {
    pub run_uuid: Uuid,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub benchmark: String,
    pub email: String,
    pub name: String,
    pub tags: Vec<(String, String)>,
    pub source: String,
}

pub const SQL_TABLE_SAMPLE: &str = r#"
    CREATE TABLE IF NOT EXISTS sample (
        sample_uuid uuid PRIMARY KEY,
        num bigint,
        status text,
        path text
    )
"#;

#[derive(Clone, Debug)]
pub struct Sample {
    pub sample_uuid: Uuid,
    pub num: i64,
    pub status: String,
    pub path: Option<String>,
}

pub const SQL_TABLE_TAG: &str = r#"
    CREATE TABLE IF NOT EXISTS tag (
        tag_uuid uuid PRIMARY KEY,
        name text,
        val text
    )
"#;

#[derive(Clone, Debug)]
pub struct Tag {
    pub tag_uuid: Uuid,
    pub name: String,
    pub val: String,
}
