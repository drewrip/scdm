use crate::args::{
    GetCommand, GetIterationArgs, GetMetricDataArgs, GetMetricDescArgs, GetParamArgs,
    GetPeriodArgs, GetRunArgs, GetSampleArgs, GetTagArgs, QueryArgs, QueryCommand,
};
use crate::cdm::*;
use anyhow::Result;
use sqlx::PgPool;
use tabled::Table;
use tabled::settings::Style;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Couldn't get the resource {0}")]
    GetError(String),
}

pub trait QueryGet {
    fn query_get(&self, pool: &PgPool) -> impl std::future::Future<Output = Result<String>> + Send;
}

impl QueryGet for GetRunArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let runs: Vec<Run> = sqlx::query_as("SELECT * FROM run").fetch_all(pool).await?;
        let mut table = Table::new(runs);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetTagArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let tags: Vec<Tag> = sqlx::query_as("SELECT * FROM tag").fetch_all(pool).await?;
        let mut table = Table::new(tags);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetIterationArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let iterations: Vec<Iteration> = sqlx::query_as("SELECT * FROM iteration")
            .fetch_all(pool)
            .await?;
        let mut table = Table::new(iterations);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetParamArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let params: Vec<Param> = sqlx::query_as("SELECT * FROM param")
            .fetch_all(pool)
            .await?;
        let mut table = Table::new(params);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetSampleArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let samples: Vec<Sample> = sqlx::query_as("SELECT * FROM sample")
            .fetch_all(pool)
            .await?;
        let mut table = Table::new(samples);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetPeriodArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let periods: Vec<Period> = sqlx::query_as("SELECT * FROM period")
            .fetch_all(pool)
            .await?;
        let mut table = Table::new(periods);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetMetricDescArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let metric_descs: Vec<MetricDesc> = sqlx::query_as("SELECT * FROM metric_desc")
            .fetch_all(pool)
            .await?;
        let mut table = Table::new(metric_descs);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

impl QueryGet for GetMetricDataArgs {
    async fn query_get(&self, pool: &PgPool) -> Result<String> {
        let metric_datas: Vec<MetricData> = sqlx::query_as("SELECT * FROM metric_data")
            .fetch_all(pool)
            .await?;
        let mut table = Table::new(metric_datas);
        table.with(Style::blank());
        Ok(table.to_string())
    }
}

pub async fn query_get(pool: &PgPool, resource: GetCommand) -> Result<()> {
    let result = match resource {
        GetCommand::Run(args) => args.query_get(pool).await,
        GetCommand::Tag(args) => args.query_get(pool).await,
        GetCommand::Iteration(args) => args.query_get(pool).await,
        GetCommand::Param(args) => args.query_get(pool).await,
        GetCommand::Sample(args) => args.query_get(pool).await,
        GetCommand::Period(args) => args.query_get(pool).await,
        GetCommand::MetricDesc(args) => args.query_get(pool).await,
        GetCommand::MetricData(args) => args.query_get(pool).await,
    }
    .map_err(|e| QueryError::GetError(e.to_string()))?;

    println!("{}", result);
    Ok(())
}

pub async fn query(pool: &PgPool, args: QueryArgs) -> Result<()> {
    match args.command {
        QueryCommand::Get(get) => query_get(pool, get.resource).await,
        QueryCommand::Delete(del) => Ok(()),
    }
}
