use chrono::{DateTime, TimeDelta, Utc};
use sqlx::prelude::FromRow;
use strum_macros::Display;
use tabled::Tabled;
use tabled::derive::display;
use uuid::Uuid;

pub const SQL_TABLE_RUN: &str = r#"
    CREATE TABLE IF NOT EXISTS run (
        run_uuid uuid PRIMARY KEY,
        begin timestamptz NOT NULL,
        finish timestamptz NOT NULL,
        benchmark text,
        email text,
        name text,
        description text,
        source text
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct Run {
    pub run_uuid: Uuid,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub benchmark: String,
    pub email: String,
    pub name: String,
    #[tabled(display("display::option", "null"))]
    pub description: Option<String>,
    pub source: String,
}

pub const SQL_TABLE_TAG: &str = r#"
    CREATE TABLE IF NOT EXISTS tag (
        run_uuid uuid REFERENCES run ON DELETE CASCADE,
        name text,
        val text,
        PRIMARY KEY (run_uuid, name)
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct Tag {
    pub run_uuid: Uuid,
    pub name: String,
    pub val: String,
}

pub const SQL_TABLE_ITERATION: &str = r#"
    CREATE TABLE IF NOT EXISTS iteration (
        iteration_uuid uuid PRIMARY KEY,
        run_uuid uuid REFERENCES run ON DELETE CASCADE,
        num bigint NOT NULL,
        status text,
        path text,
        primary_metric text NOT NULL,
        primary_period text NOT NULL
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct Iteration {
    pub iteration_uuid: Uuid,
    pub run_uuid: Uuid,
    pub num: i64,
    #[tabled(display("display::option", "null"))]
    pub status: Option<String>,
    #[tabled(display("display::option", "null"))]
    pub path: Option<String>,
    #[tabled(display("display::option", "null"))]
    pub primary_metric: Option<String>,
    #[tabled(display("display::option", "null"))]
    pub primary_period: Option<String>,
}

pub const SQL_TABLE_PARAM: &str = r#"
    CREATE TABLE IF NOT EXISTS param (
        iteration_uuid uuid REFERENCES iteration ON DELETE CASCADE,
        arg text,
        val text,
        PRIMARY KEY (iteration_uuid, arg)
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct Param {
    pub iteration_uuid: Uuid,
    pub arg: String,
    pub val: String,
}

pub const SQL_TABLE_SAMPLE: &str = r#"
    CREATE TABLE IF NOT EXISTS sample (
        sample_uuid uuid PRIMARY KEY,
        iteration_uuid uuid REFERENCES iteration ON DELETE CASCADE,
        num bigint,
        status text,
        path text
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct Sample {
    pub sample_uuid: Uuid,
    pub iteration_uuid: Uuid,
    pub num: i64,
    pub status: String,
    #[tabled(display("display::option", "null"))]
    pub path: Option<String>,
}

pub const SQL_TABLE_PERIOD: &str = r#"
    CREATE TABLE IF NOT EXISTS period (
        period_uuid uuid PRIMARY KEY,
        sample_uuid uuid REFERENCES sample ON DELETE CASCADE,
        begin timestamptz NOT NULL,
        finish timestamptz NOT NULL,
        name text
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct Period {
    pub period_uuid: Uuid,
    pub sample_uuid: Uuid,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub name: String,
}

pub const SQL_TABLE_METRIC_DESC: &str = r#"
    CREATE TABLE IF NOT EXISTS metric_desc (
        metric_desc_uuid uuid PRIMARY KEY,
        period_uuid uuid REFERENCES period ON DELETE CASCADE,
        class text NOT NULL,
        metric_type text NOT NULL,
        source text NOT NULL,
        names_list text,
        names text
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct MetricDesc {
    pub metric_desc_uuid: Uuid,
    #[tabled(display("display::option", "null"))]
    pub period_uuid: Option<Uuid>,
    pub class: String,
    pub metric_type: String,
    pub source: String,
    #[tabled(display("display::option", "null"))]
    pub names_list: Option<String>,
    #[tabled(display("display::option", "null"))]
    pub names: Option<String>,
}

pub const SQL_TABLE_METRIC_DATA: &str = r#"
    CREATE TABLE IF NOT EXISTS metric_data (
        metric_data_id bigserial,
        metric_desc_uuid uuid REFERENCES metric_desc ON DELETE CASCADE,
        value double precision NOT NULL,
        begin timestamptz NOT NULL,
        finish timestamptz NOT NULL,
        duration bigint NOT NULL,
        PRIMARY KEY (metric_data_id, metric_desc_uuid)
    )
"#;

#[derive(Clone, Debug, FromRow, Tabled)]
pub struct MetricData {
    pub metric_data_id: i64,
    pub metric_desc_uuid: Uuid,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub duration: i64,
    pub value: f64,
}
