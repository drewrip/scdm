use crate::parser::{
    IterationJson, MetricDataJson, MetricDescJson, ParamJson, PeriodJson, RunJson, SampleJson,
    insert_iterations, insert_metric_datas, insert_metric_descs, insert_params, insert_periods,
    insert_runs, insert_samples, insert_tags,
};
use crate::{args::ImportArgs, parser::TagJson};
use anyhow::Result;
use opensearch::{OpenSearch, SearchParts};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::PgPool;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum ImportError {
    #[error("Couldn't connect to the OpenSearch, {0}")]
    ConnectError(String),
    #[error("Failed to parse data from the OpenSearch, {0}")]
    ParseError(String),
    #[error("Bad arguments provided, {0}")]
    ArgError(String),
}

fn choose_query(import_args: ImportArgs) -> Result<Value, ImportError> {
    if import_args.all {
        Ok(json!({
            "query": {
                "match_all": {},
            },
        }))
    } else {
        match import_args.run_uuid {
            Some(run_uuid) => Ok(json!({
                "query": {
                    "term": {
                        "run.run-uuid": {
                            "value": run_uuid,
                        },
                    },
                },
            })),
            None => Err(ImportError::ArgError(
                "neither run_uuid or all specified".to_string(),
            )),
        }
    }
}

async fn parse_response_body<T: DeserializeOwned>(value: Value) -> Result<Vec<T>> {
    let mut resps: Vec<T> = vec![];
    for hit in value
        .get("hits")
        .ok_or(ImportError::ParseError("hits".to_string()))?
        .get("hits")
        .ok_or(ImportError::ParseError("hits.hits".to_string()))?
        .as_array()
        .ok_or(ImportError::ParseError("as_array".to_string()))?
    {
        let obj = serde_json::from_value::<T>(
            hit.get("_source")
                .ok_or(ImportError::ParseError("_source".to_string()))?
                .clone(),
        )?;
        resps.push(obj);
    }
    Ok(resps)
}

async fn request<T: DeserializeOwned>(
    client: &OpenSearch,
    index: &str,
    query: Value,
) -> Result<Vec<T>> {
    let max_results = 100000;
    let response = client
        .search(SearchParts::Index(&[index]))
        .from(0)
        .size(max_results)
        .body(query)
        .send()
        .await?;
    let response_body = response.json::<Value>().await?;
    let objs = parse_response_body(response_body).await?;
    Ok(objs)
}

pub async fn import(pool: &PgPool, args: ImportArgs) -> Result<()> {
    let client = OpenSearch::default();
    let query = choose_query(args)?;

    let runs = request::<RunJson>(&client, "cdmv8dev-run", query.clone()).await?;
    let tags = request::<TagJson>(&client, "cdmv8dev-tag", query.clone()).await?;
    let iterations = request::<IterationJson>(&client, "cdmv8dev-iteration", query.clone()).await?;
    let params = request::<ParamJson>(&client, "cdmv8dev-param", query.clone()).await?;
    let samples = request::<SampleJson>(&client, "cdmv8dev-sample", query.clone()).await?;
    let periods = request::<PeriodJson>(&client, "cdmv8dev-period", query.clone()).await?;
    let metric_descs =
        request::<MetricDescJson>(&client, "cdmv8dev-metric_desc", query.clone()).await?;
    let metric_datas =
        request::<MetricDataJson>(&client, "cdmv8dev-metric_data", query.clone()).await?;

    let mut num_new = 0;
    let mut txn = pool.begin().await?;
    num_new += insert_runs(&mut txn, &runs.iter().collect()).await?;
    num_new += insert_tags(&mut txn, &tags.iter().collect()).await?;
    num_new += insert_iterations(&mut txn, &iterations.iter().collect()).await?;
    num_new += insert_params(&mut txn, &params.iter().collect()).await?;
    num_new += insert_samples(&mut txn, &samples.iter().collect()).await?;
    num_new += insert_periods(&mut txn, &periods.iter().collect()).await?;
    num_new += insert_metric_descs(&mut txn, &metric_descs.iter().collect()).await?;
    num_new += insert_metric_datas(&mut txn, &metric_datas.iter().collect()).await?;
    txn.commit().await?;
    println!("added {} rows", num_new);
    Ok(())
}
