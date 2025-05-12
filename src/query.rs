use crate::args::{
    DeleteCommand, DeleteRunArgs, DeleteTagArgs, GetCommand, GetIterationArgs, GetMetricDataArgs,
    GetMetricDescArgs, GetNameArgs, GetParamArgs, GetPeriodArgs, GetRunArgs, GetSampleArgs,
    GetTagArgs, OutputFormat, QueryArgs, QueryCommand,
};
use crate::cdm::*;
use crate::metric::query_metric;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use sqlx::prelude::FromRow;
use tabled::derive::display;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use thiserror::Error;
use uuid::Uuid;

pub const PG_VAR_NUM_LIMIT: i32 = 65535;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Couldn't get the resource, {0}")]
    GetError(String),
    #[error("Couldn't serialize the results to the format {0}")]
    SerializeError(String),
    #[error("Unknown format {0}")]
    UnknownFormat(String),
    #[error("Couldn't delete the resource, {0}")]
    DeleteError(String),
    #[error("Couldn't get the metrics, {0}")]
    MetricError(String),
}

pub trait QueryGet<T>
where
    T: Serialize + Tabled,
{
    fn query_get(
        &self,
        pool: &PgPool,
    ) -> impl std::future::Future<Output = Result<Vec<T>, QueryError>>;

    fn query_json(
        &self,
        pool: &PgPool,
    ) -> impl std::future::Future<Output = Result<String, QueryError>> {
        async {
            let results: Vec<T> = self.query_get(pool).await?;
            Ok(serde_json::to_string_pretty::<Vec<T>>(&results)
                .map_err(|e| QueryError::SerializeError(format!("JSON ({})", e.to_string())))?)
        }
    }

    fn query_csv(
        &self,
        pool: &PgPool,
    ) -> impl std::future::Future<Output = Result<String, QueryError>> {
        async {
            let results: Vec<T> = self.query_get(pool).await?;
            let mut writer = csv::Writer::from_writer(vec![]);
            for result in &results {
                writer
                    .serialize(result)
                    .map_err(|e| QueryError::SerializeError(format!("CSV ({})", e.to_string())))?;
            }
            Ok(String::from_utf8(
                writer
                    .into_inner()
                    .map_err(|e| QueryError::SerializeError(format!("CSV ({})", e.to_string())))?,
            )
            .map_err(|e| QueryError::SerializeError(format!("CSV ({})", e.to_string())))?)
        }
    }

    fn query_table(
        &self,
        pool: &PgPool,
    ) -> impl std::future::Future<Output = Result<String, QueryError>> {
        async {
            let results: Vec<T> = self.query_get(pool).await?;
            let mut table = Table::new(results);
            table.with(Style::modern());
            Ok(table.to_string())
        }
    }
}

impl QueryGet<Run> for GetRunArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Run>, QueryError> {
        let raw_query: &str = r#"
            SELECT run.* FROM run LEFT JOIN tag ON run.run_uuid = tag.run_uuid
            WHERE
                ($1 IS NULL OR run.run_uuid = $1) AND
                ($2 IS NULL OR begin <= $2) AND
                ($3 IS NULL OR begin >= $3) AND
                ($4 IS NULL OR finish <= $4) AND
                ($5 IS NULL OR finish >= $5) AND
                ($6 IS NULL OR benchmark = $6) AND
                ($7 IS NULL OR email = $7) AND
                ($8 IS NULL OR run.name = $8) AND
                ($9 IS NULL OR source = $9) AND
                ($10 IS NULL OR tag.name = $10) AND
                ($11 IS NULL OR tag.val = $11)
            "#;

        let (tag_name, tag_value): (Option<String>, Option<String>) =
            if let Some(maybe_tag) = self.tag.clone() {
                let parts: Vec<String> = maybe_tag.split("=").map(|s| s.to_string()).collect();
                (parts.get(0).cloned(), parts.get(1).cloned())
            } else {
                (None, None)
            };
        let query = sqlx::query_as(raw_query)
            .bind(self.run_uuid)
            .bind(self.begin_before)
            .bind(self.begin_after)
            .bind(self.finish_before)
            .bind(self.finish_after)
            .bind(self.benchmark.clone())
            .bind(self.email.clone())
            .bind(self.name.clone())
            .bind(self.source.clone())
            .bind(tag_name)
            .bind(tag_value);
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<Tag> for GetTagArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Tag>, QueryError> {
        let raw_query: &str = r#"
            SELECT * FROM tag
            WHERE
                ($1 IS NULL OR run_uuid = $1) AND
                ($2 IS NULL OR name = $2) AND
                ($3 IS NULL OR val = $3)
            "#;

        let (tag_name, tag_value): (Option<String>, Option<String>) =
            if let Some(maybe_tag) = self.tag.clone() {
                let parts: Vec<String> = maybe_tag.split("=").map(|s| s.to_string()).collect();
                (parts.get(0).cloned(), parts.get(1).cloned())
            } else {
                (None, None)
            };

        let query = sqlx::query_as(raw_query)
            .bind(self.run_uuid)
            .bind(tag_name)
            .bind(tag_value);
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<Iteration> for GetIterationArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Iteration>, QueryError> {
        let raw_query: &str = r#"
            SELECT iteration.* FROM iteration
            WHERE
                ($1 IS NULL OR iteration_uuid = $1) AND
                ($2 IS NULL OR run_uuid = $2) AND
                ($3 IS NULL OR num = $3) AND
                ($4 IS NULL OR status = $4)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.iteration_uuid)
            .bind(self.run_uuid)
            .bind(self.num)
            .bind(self.status.clone());
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<Param> for GetParamArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Param>, QueryError> {
        let raw_query: &str = r#"
            SELECT param.* FROM param
            WHERE
                ($1 IS NULL OR iteration_uuid = $1) AND
                ($2 IS NULL OR arg = $2) AND
                ($3 IS NULL OR val = $3)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.iteration_uuid)
            .bind(self.arg.clone())
            .bind(self.val.clone());
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<Sample> for GetSampleArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Sample>, QueryError> {
        let raw_query: &str = r#"
            SELECT sample.* FROM sample
            WHERE
                ($1 IS NULL OR sample_uuid = $1) AND
                ($2 IS NULL OR iteration_uuid = $2) AND
                ($3 IS NULL OR num = $3) AND
                ($3 IS NULL OR status = $4)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.sample_uuid)
            .bind(self.iteration_uuid)
            .bind(self.num)
            .bind(self.status.clone());
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<Period> for GetPeriodArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Period>, QueryError> {
        let raw_query: &str = r#"
            SELECT period.* FROM period
            WHERE
                ($1 IS NULL OR period_uuid = $1) AND
                ($2 IS NULL OR sample_uuid = $2) AND
                ($3 IS NULL OR begin <= $3) AND
                ($4 IS NULL OR begin >= $4) AND
                ($5 IS NULL OR finish <= $5) AND
                ($6 IS NULL OR finish >= $6) AND
                ($7 IS NULL OR name = $7)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.period_uuid)
            .bind(self.sample_uuid)
            .bind(self.begin_before)
            .bind(self.begin_after)
            .bind(self.finish_before)
            .bind(self.finish_after)
            .bind(self.name.clone());
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<MetricDesc> for GetMetricDescArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<MetricDesc>, QueryError> {
        let raw_query: &str = r#"
            SELECT metric_desc.* FROM metric_desc
            WHERE
                ($1 IS NULL OR metric_desc_uuid = $1) AND
                ($2 IS NULL OR period_uuid = $2) AND
                ($3 IS NULL OR class = $3) AND
                ($4 IS NULL OR metric_type = $4) AND
                ($5 IS NULL OR source = $5)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.metric_desc_uuid)
            .bind(self.period_uuid)
            .bind(self.class.clone())
            .bind(self.metric_type.clone())
            .bind(self.source.clone());
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

impl QueryGet<Name> for GetNameArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Name>, QueryError> {
        let raw_query: &str = r#"
            SELECT name.* FROM name
            WHERE
                ($1 IS NULL OR metric_desc_uuid = $1) AND
                ($2 IS NULL OR name = $2) AND
                ($3 IS NULL OR val = $3)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.metric_desc_uuid)
            .bind(self.name.clone())
            .bind(self.val.clone());
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

#[derive(Clone, Debug, FromRow, Tabled, Serialize)]
pub struct Data {
    #[tabled(display("display::option", "null"))]
    pub run_uuid: Option<Uuid>,
    #[tabled(display("display::option", "null"))]
    pub iteration_uuid: Option<Uuid>,
    pub metric_desc_uuid: Uuid,
    pub metric_type: String,
    pub begin: DateTime<Utc>,
    pub finish: DateTime<Utc>,
    pub duration: i64,
    pub value: f64,
}

impl QueryGet<Data> for GetMetricDataArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<Vec<Data>, QueryError> {
        let raw_query: &str = r#"
            SELECT
                run.run_uuid as run_uuid,
                iteration.iteration_uuid as iteration_uuid,
                metric_desc.metric_type as metric_type,
                metric_data.*
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
            WHERE
                ($1 IS NULL OR run.run_uuid = $1) AND
                ($2 IS NULL OR iteration.iteration_uuid = $2) AND
                ($3 IS NULL OR metric_data.metric_desc_uuid = $3) AND
                ($4 IS NULL OR metric_desc.metric_type = $4) AND
                ($5 IS NULL OR metric_data.begin <= $5) AND
                ($6 IS NULL OR metric_data.begin >= $6) AND
                ($7 IS NULL OR metric_data.finish <= $7) AND
                ($8 IS NULL OR metric_data.finish >= $8) AND
                ($9 IS NULL OR metric_data.value = $9) AND
                ($10 IS NULL OR metric_data.value < $10) AND
                ($11 IS NULL OR metric_data.value > $11)
            "#;

        let query = sqlx::query_as(raw_query)
            .bind(self.run_uuid)
            .bind(self.iteration_uuid)
            .bind(self.metric_desc_uuid)
            .bind(self.metric_type.clone())
            .bind(self.begin_before)
            .bind(self.begin_after)
            .bind(self.finish_before)
            .bind(self.finish_after)
            .bind(self.value_eq)
            .bind(self.value_lt)
            .bind(self.value_gt);
        Ok(query
            .fetch_all(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?)
    }
}

pub async fn query_get<T: Serialize + Tabled, U: QueryGet<T>>(
    pool: &PgPool,
    resource: U,
    format: Option<OutputFormat>,
) -> Result<()> {
    let result: String = match format {
        Some(format_type) => match format_type {
            OutputFormat::JSON => resource.query_json(pool).await,
            OutputFormat::CSV => resource.query_csv(pool).await,
        },
        None => resource.query_table(pool).await,
    }?;

    println!("{}", result);
    Ok(())
}

pub trait QueryDelete {
    fn query_delete(
        &self,
        pool: &PgPool,
    ) -> impl std::future::Future<Output = Result<u64, QueryError>>;
}

impl QueryDelete for DeleteRunArgs {
    async fn query_delete(&self, pool: &PgPool) -> Result<u64, QueryError> {
        let raw_query: &str = r#"
            DELETE FROM run
            USING run AS r
            LEFT JOIN tag as t ON
                r.run_uuid = t.run_uuid
            WHERE
                (run.run_uuid = r.run_uuid) AND
                ($1 IS NULL OR run.run_uuid = $1) AND
                ($2 IS NULL OR run.begin <= $2) AND
                ($3 IS NULL OR run.begin >= $3) AND
                ($4 IS NULL OR run.finish <= $4) AND
                ($5 IS NULL OR run.finish >= $5) AND
                ($6 IS NULL OR run.benchmark = $6) AND
                ($7 IS NULL OR run.email = $7) AND
                ($8 IS NULL OR run.name = $8) AND
                ($9 IS NULL OR run.source = $9) AND
                ($10 IS NULL OR t.name = $10) AND
                ($11 IS NULL OR t.val = $11)
            "#;

        let (tag_name, tag_value): (Option<String>, Option<String>) =
            if let Some(maybe_tag) = self.tag.clone() {
                let parts: Vec<String> = maybe_tag.split("=").map(|s| s.to_string()).collect();
                (parts.get(0).cloned(), parts.get(1).cloned())
            } else {
                (None, None)
            };
        let query = sqlx::query(raw_query)
            .bind(self.run_uuid)
            .bind(self.begin_before)
            .bind(self.begin_after)
            .bind(self.finish_before)
            .bind(self.finish_after)
            .bind(self.benchmark.clone())
            .bind(self.email.clone())
            .bind(self.name.clone())
            .bind(self.source.clone())
            .bind(tag_name)
            .bind(tag_value);

        let results = query
            .execute(pool)
            .await
            .map_err(|e| QueryError::DeleteError(format!("{}", e)))?;
        Ok(results.rows_affected())
    }
}

impl QueryDelete for DeleteTagArgs {
    async fn query_delete(&self, pool: &PgPool) -> Result<u64, QueryError> {
        let raw_query: &str = r#"
            DELETE FROM tag
            WHERE
                ($1 IS NULL OR run_uuid = $1) AND
                ($2 IS NULL OR name = $2) AND
                ($3 IS NULL OR val = $3)
            "#;

        let (tag_name, tag_value): (Option<String>, Option<String>) =
            if let Some(maybe_tag) = self.tag.clone() {
                let parts: Vec<String> = maybe_tag.split("=").map(|s| s.to_string()).collect();
                (parts.get(0).cloned(), parts.get(1).cloned())
            } else {
                (None, None)
            };

        let query = sqlx::query(raw_query)
            .bind(self.run_uuid)
            .bind(tag_name)
            .bind(tag_value);
        let results = query
            .execute(pool)
            .await
            .map_err(|e| QueryError::GetError(format!("{}", e)))?;
        Ok(results.rows_affected())
    }
}

pub async fn query_delete<U: QueryDelete>(pool: &PgPool, resource: U) -> Result<()> {
    let num_deletes = resource.query_delete(pool).await?;
    println!("deleted {} rows", num_deletes);
    Ok(())
}

pub async fn query(pool: &PgPool, args: QueryArgs) -> Result<()> {
    match args.command {
        QueryCommand::Get(get) => match get.resource {
            GetCommand::Run(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::Tag(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::Iteration(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::Param(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::Sample(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::Period(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::MetricDesc(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::MetricData(args) => query_get(pool, args, get.get_options.output).await,
            GetCommand::Name(args) => query_get(pool, args, get.get_options.output).await,
        },
        QueryCommand::Delete(del) => match del.resource {
            DeleteCommand::Run(args) => query_delete(pool, args).await,
            DeleteCommand::Tag(args) => query_delete(pool, args).await,
        },
        QueryCommand::Metric(metric_args) => query_metric(pool, metric_args).await,
    }
}
