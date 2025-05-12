use std::fmt;

use crate::args::{Aggregator, MetricArgs};
use crate::query::QueryError;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::prelude::FromRow;
use sqlx::{Column, PgPool, Postgres, QueryBuilder, Row};
use tabled::derive::display;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
pub enum CellValue {
    String(String),
    Number(Number),
    Null,
}

#[derive(Clone, Debug, Serialize)]
pub enum Number {
    Float(f64),
    Int(i128),
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Number::Float(n) => write!(f, "{}", n),
            Number::Int(n) => write!(f, "{}", n),
        }
    }
}

impl fmt::Display for CellValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CellValue::String(s) => write!(f, "{}", s),
            CellValue::Number(n) => write!(f, "{}", n),
            CellValue::Null => write!(f, "null"),
        }
    }
}

pub fn unpack_rows(
    pg_rows: Vec<PgRow>,
    names: &Vec<(String, Option<String>)>,
) -> (Vec<String>, Vec<Vec<String>>) {
    let mut results: Vec<Vec<String>> = Vec::new();
    for pg_row in &pg_rows {
        let run_uuid: Uuid = pg_row.try_get("run_uuid").unwrap_or(Uuid::nil());
        let iteration_uuid: Uuid = pg_row.try_get("iteration_uuid").unwrap_or(Uuid::nil());
        let metric_type: String = pg_row.try_get("metric_type").unwrap_or("null".to_string());
        let mut row: Vec<String> = vec![
            run_uuid.to_string(),
            iteration_uuid.to_string(),
            metric_type,
        ];
        let mut next_idx = 3;
        for _ in names {
            row.push(pg_row.get(next_idx));
            next_idx += 1;
        }
        let value: f64 = pg_row.try_get(next_idx).unwrap_or(0.0);
        row.push(value.to_string()); // aggregated value
        results.push(row);
    }
    let header: Vec<String> = pg_rows
        .iter()
        .take(1)
        .map(|r| {
            r.columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<Vec<String>>()
        })
        .flatten()
        .collect();
    (header, results)
}

fn push_choose_aggregator(
    qb: &mut QueryBuilder<Postgres>,
    agg: Aggregator,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
) {
    match agg {
        Aggregator::None => {
            qb.push("metric_data.value as value");
        }
        Aggregator::Avg => {
            qb.push("AVG(metric_data.value) as avg");
        }
        Aggregator::WeightedAvg => {
            let duration_correction = match window {
                Some((begin, finish)) => format!(r#"
                        (
                            metric_data.duration
                                - (EXTRACT(EPOCH FROM (metric_data.begin))::bigint * 1000 - {})
                                - ({} - EXTRACT(EPOCH FROM (metric_data.finish))::bigint * 1000)
                        )
                    "#, begin.timestamp_millis(), finish.timestamp_millis()).to_string(),
                None => {
                    r#"
                        (
                            metric_data.duration
                                - (EXTRACT(EPOCH FROM (metric_data.begin))::bigint * 1000 - EXTRACT(EPOCH FROM (poi.begin))::bigint * 1000)
                                - (EXTRACT(EPOCH FROM (poi.finish))::bigint * 1000 - EXTRACT(EPOCH FROM (metric_data.finish))::bigint * 1000)
                        )
                    "#.to_string()
                }
            };
            qb.push("SUM(metric_data.value * ");
            qb.push(&duration_correction);
            qb.push(" ) / SUM( ");
            qb.push(duration_correction);
            qb.push(" ) as weighted_avg");
        }
        Aggregator::Stddev => {
            qb.push("STDDEV(metric_data.value) as stddev");
        }
        Aggregator::Min => {
            qb.push("MIN(metric_data.value) as min");
        }
        Aggregator::Max => {
            qb.push("MAX(metric_data.value) as max");
        }
    };
}

#[derive(Clone, Debug, FromRow, Tabled, Serialize)]
pub struct Metric {
    pub run_uuid: Uuid,
    pub iteration_uuid: Uuid,
    pub metric_type: String,
    #[tabled(display("display::option", "null"))]
    pub name: Option<String>,
    #[tabled(display("display::option", "null"))]
    pub val: Option<String>,
    pub value: f64,
}

fn push_metric_subquery(
    qb: &mut QueryBuilder<Postgres>,
    maybe_name: Option<String>,
    maybe_value: Option<String>,
) {
    let subquery_part: &str = r#"
        (SELECT
            name.metric_desc_uuid as metric_desc_uuid,
            metric_desc.metric_type as metric_type,
            name.val as name_value,
            metric_data.value as metric_value
        FROM metric_desc, name, metric_data
        WHERE
            metric_desc.metric_desc_uuid = name.metric_desc_uuid AND
            name.metric_desc_uuid = metric_data.metric_desc_uuid
    "#;
    qb.push(subquery_part);
    if let Some(name) = maybe_name.clone() {
        qb.push(" AND name.name = ");
        qb.push_bind(name.clone());
    }
    if let Some(value) = maybe_value {
        qb.push(" AND name.val = ");
        qb.push_bind(value.clone());
    }
    qb.push(format!(
        ") as \"{}\"",
        maybe_name.unwrap_or("base".to_string())
    ));
}

pub async fn query_metric(pool: &PgPool, metric_args: MetricArgs) -> Result<()> {
    let mut names: Vec<(String, Option<String>)> = Vec::new();
    for name in metric_args.name.clone().unwrap_or(vec![]) {
        let parts: Vec<String> = name.split("=").map(|s| s.to_string()).collect();
        let n = parts
            .get(0)
            .ok_or(QueryError::MetricError(format!(
                "invalid name, {:?}",
                parts
            )))?
            .to_string();
        let v = parts.get(1);
        names.push((n, v.cloned()));
    }

    let (base_name, base_value) = match names.clone().first() {
        Some(first) => (first.clone().0, first.clone().1),
        None => ("base".to_string(), None),
    };
    let select_part: &str = r#"
        SELECT
            run.run_uuid as run_uuid,
            iteration.iteration_uuid as iteration_uuid,
            metric_desc.metric_type as metric_type,
    "#;

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(select_part);
    for (name, _) in &names {
        qb.push(format!(" \"{}\".name_value as \"{}_v\" ", name, name));
        qb.push(", ");
    }
    let window = if let (Some(begin), Some(finish)) = (metric_args.begin, metric_args.finish) {
        Some((begin, finish))
    } else {
        None
    };

    push_choose_aggregator(&mut qb, metric_args.aggregator.clone(), window);

    let join_part: &str = r#"
        FROM metric_data
        LEFT JOIN metric_desc
            ON metric_desc.metric_desc_uuid = metric_data.metric_desc_uuid
        LEFT JOIN period
            ON period.period_uuid = metric_desc.period_uuid
        LEFT JOIN sample
            ON sample.sample_uuid = period.sample_uuid
        LEFT JOIN iteration
            ON iteration.iteration_uuid = sample.iteration_uuid
        LEFT JOIN run
            ON run.run_uuid = iteration.run_uuid
        LEFT JOIN
    "#;

    qb.push(join_part);

    push_metric_subquery(&mut qb, Some(base_name.clone()), base_value);
    if names.len() > 1 {
        qb.push(" ON ");
        qb.push(format!(
            " metric_desc.metric_desc_uuid = \"{}\".metric_desc_uuid",
            base_name
        ));
        qb.push(" LEFT JOIN ");
    }
    let mut last_name = base_name.clone();
    for (i, (name, maybe_value)) in names.clone().into_iter().enumerate().skip(1) {
        push_metric_subquery(&mut qb, Some(name.clone()), maybe_value);
        qb.push(" ON ");
        qb.push(format!(
            " \"{}\".metric_desc_uuid = \"{}\".metric_desc_uuid",
            last_name, name
        ));
        last_name = name;
        if i < names.len() - 1 {
            qb.push(" LEFT JOIN ");
        }
    }

    if let Some(ref_period) = metric_args.ref_period {
        qb.push(" , (SELECT begin, finish FROM period WHERE period_uuid = ");
        qb.push_bind(ref_period);
        qb.push(") as poi");
    }

    qb.push(" WHERE ");
    let mut sep = qb.separated(" AND ");
    sep.push(" TRUE ");
    if let Some(run_uuid) = metric_args.run_uuid {
        sep.push(" run.run_uuid = ");
        sep.push_bind_unseparated(run_uuid);
    }
    if let Some(iteration_uuid) = metric_args.iteration_uuid {
        sep.push(" iteration.iteration_uuid = ");
        sep.push_bind_unseparated(iteration_uuid);
    }
    if let Some(metric_desc_uuid) = metric_args.metric_desc_uuid {
        sep.push(" metric_data.metric_desc_uuid = ");
        sep.push_bind_unseparated(metric_desc_uuid);
    }
    if let Some(metric_type) = metric_args.metric_type {
        sep.push(" metric_desc.metric_type = ");
        sep.push_bind_unseparated(metric_type.clone());
    }
    if let Some(value_eq) = metric_args.value_eq {
        sep.push(" metric_data.value = ");
        sep.push_bind_unseparated(value_eq);
    }
    if let Some(value_lt) = metric_args.value_lt {
        sep.push(" metric_data.value < ");
        sep.push_bind_unseparated(value_lt);
    }
    if let Some(value_gt) = metric_args.value_gt {
        sep.push(" metric_data.value > ");
        sep.push_bind_unseparated(value_gt);
    }

    if metric_args.ref_period.is_some() {
        sep.push(
            r#"
        (
            (metric_data.begin > poi.begin AND metric_data.begin < poi.finish) OR
            (metric_data.finish > poi.begin AND metric_data.finish < poi.finish) OR
            (metric_data.begin < poi.begin AND metric_data.finish > poi.finish)
        )
        "#,
        );
    }
    if let (Some(begin), Some(finish)) = (metric_args.begin, metric_args.finish) {
        sep.push(" ( ");
        sep.push_unseparated("( metric_data.begin > ");
        sep.push_bind_unseparated(begin);
        sep.push_unseparated(" AND metric_data.begin < ");
        sep.push_bind_unseparated(finish);
        sep.push_unseparated(" ) OR ");

        sep.push_unseparated("( metric_data.finish > ");
        sep.push_bind_unseparated(begin);
        sep.push_unseparated(" AND metric_data.finish < ");
        sep.push_bind_unseparated(finish);
        sep.push_unseparated(" ) OR ");

        sep.push_unseparated("( metric_data.begin < ");
        sep.push_bind_unseparated(begin);
        sep.push_unseparated(" AND metric_data.finish > ");
        sep.push_bind_unseparated(finish);
        sep.push_unseparated(" )");

        sep.push_unseparated(" ) ");
    }

    if metric_args.name.is_some() && !matches!(metric_args.aggregator, Aggregator::None) {
        qb.push(" GROUP BY ");
        let mut sep = qb.separated(", ");
        sep.push("run.run_uuid");
        sep.push("iteration.iteration_uuid");
        sep.push("metric_desc.metric_type");
        for (name, _) in names.clone() {
            sep.push(format!("\"{}\".name_value", name));
        }
    }

    if metric_args.name.is_some() && !matches!(metric_args.aggregator, Aggregator::None) {
        qb.push(" ORDER BY ");
        let mut sep = qb.separated(", ");
        for (name, _) in &names {
            sep.push(format!("\"{}\".name_value", name));
        }
    }

    let query = qb.build();
    let res = query
        .fetch_all(pool)
        .await
        .map_err(|e| QueryError::MetricError(format!("{}", e)))?;

    let (header, rows) = unpack_rows(res, &names);
    let mut table = Table::from_iter(vec![header].into_iter().chain(rows));
    table.with(Style::modern());
    println!("{}", table.to_string());
    Ok(())
}
