use std::collections::HashMap;

use crate::parser::{
    GlobalResource, IterationJson, MetricDataJson, MetricDescJson, ParamJson, PeriodJson, RunJson,
    SampleJson, insert_iterations, insert_metric_datas, insert_metric_descs, insert_params,
    insert_periods, insert_runs, insert_samples, insert_tags,
};
use crate::{args::ImportArgs, parser::TagJson};
use anyhow::Result;
use opensearch::{OpenSearch, SearchParts};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug, Clone)]
pub enum ImportError {
    #[error("Couldn't connect to the OpenSearch, {0}")]
    ConnectError(String),
    #[error("Failed to parse data from the OpenSearch, {0}")]
    ParseError(String),
    #[error("Bad arguments provided, {0}")]
    ArgError(String),
}

fn build_queries(run_uuid: Option<Vec<Uuid>>) -> Vec<Value> {
    match run_uuid {
        Some(uuids) => uuids
            .iter()
            .map(|u| {
                json!({
                    "query": {
                        "term": {
                            "run.run-uuid": {
                                "value": u,
                            },
                        },
                    },
                })
            })
            .collect(),
        None => vec![json!({
            "query": {
                "match_all": {},
            },
        })],
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

    let queries = build_queries(args.run_uuid);

    for query in queries {
        let runs = request::<RunJson>(&client, "cdmv8dev-run", query.clone()).await?;
        let tags = request::<TagJson>(&client, "cdmv8dev-tag", query.clone()).await?;
        let mut iterations =
            request::<IterationJson>(&client, "cdmv8dev-iteration", query.clone()).await?;
        let params = request::<ParamJson>(&client, "cdmv8dev-param", query.clone()).await?;
        let mut samples = request::<SampleJson>(&client, "cdmv8dev-sample", query.clone()).await?;
        let mut periods = request::<PeriodJson>(&client, "cdmv8dev-period", query.clone()).await?;
        let mut metric_descs =
            request::<MetricDescJson>(&client, "cdmv8dev-metric_desc", query.clone()).await?;
        let mut metric_datas =
            request::<MetricDataJson>(&client, "cdmv8dev-metric_data", query.clone()).await?;

        let mut num_new = 0;
        let mut txn = pool.begin().await?;
        // Default resources for data that is scoped to the run
        let mut globals: HashMap<Uuid, GlobalResource> = HashMap::new();

        let (
            new_run_rows,
            mut global_iterations,
            mut global_samples,
            mut global_periods,
            mut global_metric_descs,
            mut global_metric_datas,
        ) = insert_runs(&mut txn, &mut globals, &runs.iter().collect()).await?;
        iterations.append(&mut global_iterations);
        samples.append(&mut global_samples);
        periods.append(&mut global_periods);
        metric_descs.append(&mut global_metric_descs);
        metric_datas.append(&mut global_metric_datas);
        num_new += new_run_rows;

        num_new += insert_tags(&mut txn, &tags.iter().collect()).await?;
        num_new += insert_iterations(&mut txn, &iterations.iter().collect()).await?;
        num_new += insert_params(&mut txn, &params.iter().collect()).await?;
        num_new += insert_samples(&mut txn, &samples.iter().collect()).await?;
        num_new += insert_periods(&mut txn, &periods.iter().collect()).await?;
        num_new += insert_metric_descs(&mut txn, &globals, &metric_descs.iter().collect()).await?;
        num_new += insert_metric_datas(&mut txn, &metric_datas.iter().collect()).await?;
        txn.commit().await?;
        println!("added {} rows", num_new);
    }
    Ok(())
}
