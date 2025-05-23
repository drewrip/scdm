use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

use crate::SCDMError;

/// SCDM: Structured Common Data Model -
/// A tool to index and query performance metrics that come from Crucible runs.
#[derive(Debug, Parser)]
#[clap(name = "scdm", version)]
pub struct App {
    #[clap(flatten)]
    pub global_opts: GlobalOpts,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Args)]
pub struct GlobalOpts {
    /// The DB_USER Env variable takes precedence
    #[clap(long = "db-user", short = 'u')]
    pub db_user: Option<String>,

    /// The DB_PASSWORD Env variable takes precedence
    #[clap(long = "db-password", short = 'p')]
    pub db_password: Option<String>,

    /// The DB_URL Env variable takes precedence
    #[clap(long = "db-url")]
    pub db_url: Option<String>,

    /// The DB_PORT Env variable takes precedence
    #[clap(long = "db-port")]
    pub db_port: Option<String>,

    /// The DB_NAME Env variable takes precedence
    #[clap(long = "db-name", default_value = "scdm")]
    pub db_name: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Parse the results of a crucible iteration and import into DB
    Parse(ParseArgs),
    /// Add the results from the JSON file
    Add(AddArgs),
    /// Query the the CDM DB
    Query(QueryArgs),
    /// Import run from OpenSearch CDM DB
    Import(ImportArgs),
    /// Init the SCDM tables if they don't exist
    Init,
}

#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
pub struct ImportArgs {
    #[clap(long = "run-uuid", value_delimiter = ',')]
    pub run_uuid: Option<Vec<Uuid>>,
    #[clap(long = "all", action)]
    pub all: bool,
}

#[derive(Debug, Args)]
pub struct ParseArgs {
    pub path: String,
}

#[derive(Debug, Args)]
pub struct AddArgs {
    pub path: String,
}

#[derive(Debug, Args)]
pub struct QueryArgs {
    #[clap(subcommand)]
    pub command: QueryCommand,
}

#[derive(Debug, Subcommand)]
pub enum QueryCommand {
    /// Retrieve information about a CDM resource
    Get(GetArgs),
    /// Delete a CDM resource
    Delete(DeleteArgs),
    /// Query the data
    Metric(MetricArgs),
}

#[derive(Debug, Args)]
#[command(
    subcommand_value_name = "resource",
    subcommand_help_heading = "Resources"
)]
pub struct GetArgs {
    #[clap(subcommand)]
    pub resource: GetCommand,
    #[clap(flatten)]
    pub get_options: GetOptions,
}

#[derive(Debug, Args)]
pub struct GetOptions {
    #[clap(long = "output", short = 'o')]
    pub output: Option<OutputFormat>,
}

#[derive(Debug, ValueEnum, Clone)]
pub enum OutputFormat {
    JSON,
    CSV,
}

#[derive(Debug, Subcommand)]
pub enum GetCommand {
    Run(GetRunArgs),
    Tag(GetTagArgs),
    Iteration(GetIterationArgs),
    Param(GetParamArgs),
    Sample(GetSampleArgs),
    Period(GetPeriodArgs),
    MetricDesc(GetMetricDescArgs),
    MetricData(GetMetricDataArgs),
    Name(GetNameArgs),
}

fn parse_timestamp(arg: &str) -> Result<DateTime<Utc>, SCDMError> {
    if let Ok(human_readable) = arg.parse::<DateTime<Utc>>() {
        Ok(human_readable)
    } else {
        let n: i64 = arg
            .parse()
            .map_err(|_| SCDMError::FailedTimestampParse(arg.to_string()))?;

        let res = DateTime::from_timestamp_millis(n)
            .ok_or(SCDMError::FailedTimestampParse(arg.to_string()))?;
        Ok(res)
    }
}

#[derive(Debug, Args)]
pub struct GetRunArgs {
    #[clap(long = "run-uuid", short = 'u')]
    pub run_uuid: Option<Uuid>,
    /// Search for runs where "tag_name=tag_value"
    #[clap(long = "tag", short = 't')]
    pub tag: Option<String>,
    /// Search for runs that begin before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-before", short = 'b', value_parser = parse_timestamp)]
    pub begin_before: Option<DateTime<Utc>>,
    /// Search for runs that begin after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-after", value_parser = parse_timestamp)]
    pub begin_after: Option<DateTime<Utc>>,
    /// Search for runs that finish before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-before", short = 'f', value_parser = parse_timestamp)]
    pub finish_before: Option<DateTime<Utc>>,
    /// Search for runs that finish after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-after", value_parser = parse_timestamp)]
    pub finish_after: Option<DateTime<Utc>>,
    #[clap(long = "benchmark", short = 'k')]
    pub benchmark: Option<String>,
    #[clap(long = "email", short = 'e')]
    pub email: Option<String>,
    #[clap(long = "name", short = 'n')]
    pub name: Option<String>,
    #[clap(long = "source", short = 's')]
    pub source: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetTagArgs {
    #[clap(long = "run-uuid", short = 'r')]
    pub run_uuid: Option<Uuid>,
    /// Search for runs where "tag_name=tag_value"
    #[clap(long = "tag", short = 't')]
    pub tag: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetIterationArgs {
    #[clap(long = "iteration-uuid", short = 'u')]
    pub iteration_uuid: Option<Uuid>,
    #[clap(long = "run-uuid", short = 'r')]
    pub run_uuid: Option<Uuid>,
    #[clap(long = "num", short = 'n')]
    pub num: Option<i64>,
    #[clap(long = "status", short = 's')]
    pub status: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetParamArgs {
    #[clap(long = "iteration_uuid", short = 'i')]
    pub iteration_uuid: Option<Uuid>,
    #[clap(long = "arg", short = 'a')]
    pub arg: Option<String>,
    #[clap(long = "value", short = 'v')]
    pub val: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetSampleArgs {
    #[clap(long = "sample-uuid", short = 'u')]
    pub sample_uuid: Option<Uuid>,
    #[clap(long = "iteration-uuid", short = 'i')]
    pub iteration_uuid: Option<Uuid>,
    #[clap(long = "num", short = 'n')]
    pub num: Option<i64>,
    #[clap(long = "status", short = 's')]
    pub status: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetPeriodArgs {
    #[clap(long = "period-uuid", short = 'u')]
    pub period_uuid: Option<Uuid>,
    #[clap(long = "sample-uuid", short = 's')]
    pub sample_uuid: Option<Uuid>,
    /// Search for periods that begin before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-before", short = 'b', value_parser = parse_timestamp)]
    pub begin_before: Option<DateTime<Utc>>,
    /// Search for periods that begin after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-after", value_parser = parse_timestamp)]
    pub begin_after: Option<DateTime<Utc>>,
    /// Search for periods that finish before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-before", short = 'f', value_parser = parse_timestamp)]
    pub finish_before: Option<DateTime<Utc>>,
    /// Search for periods that finish after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-after", value_parser = parse_timestamp)]
    pub finish_after: Option<DateTime<Utc>>,
    #[clap(long = "name", short = 'n')]
    pub name: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetMetricDescArgs {
    #[clap(long = "metric-desc-uuid", short = 'u')]
    pub metric_desc_uuid: Option<Uuid>,
    #[clap(long = "period-uuid", short = 'p')]
    pub period_uuid: Option<Uuid>,
    #[clap(long = "class", short = 'c')]
    pub class: Option<String>,
    #[clap(long = "metric-type", short = 'm')]
    pub metric_type: Option<String>,
    #[clap(long = "source", short = 's')]
    pub source: Option<String>,
}

#[derive(Debug, Args)]
pub struct GetMetricDataArgs {
    #[clap(long = "run-uuid", short = 'r')]
    pub run_uuid: Option<Uuid>,
    #[clap(long = "iteration-uuid", short = 'i')]
    pub iteration_uuid: Option<Uuid>,
    #[clap(long = "metric-desc-uuid", short = 'm')]
    pub metric_desc_uuid: Option<Uuid>,
    #[clap(long = "metric-type", short = 't')]
    pub metric_type: Option<String>,
    /// Search for data that begins before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-before", short = 'b', value_parser = parse_timestamp)]
    pub begin_before: Option<DateTime<Utc>>,
    /// Search for data that begins after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-after", value_parser = parse_timestamp)]
    pub begin_after: Option<DateTime<Utc>>,
    /// Search for data that finishes before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-before", short = 'f', value_parser = parse_timestamp)]
    pub finish_before: Option<DateTime<Utc>>,
    /// Search for data that finishes after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-after", value_parser = parse_timestamp)]
    pub finish_after: Option<DateTime<Utc>>,
    #[clap(long = "value-eq")]
    /// Search for values equal to
    pub value_eq: Option<f64>,
    #[clap(long = "value-lt")]
    /// Search for values less than
    pub value_lt: Option<f64>,
    /// Search for values greater than
    #[clap(long = "value-gt")]
    pub value_gt: Option<f64>,
}

#[derive(Debug, Args)]
pub struct GetNameArgs {
    #[clap(long = "iteration_uuid", short = 'i')]
    pub metric_desc_uuid: Option<Uuid>,
    #[clap(long = "name", short = 'n')]
    pub name: Option<String>,
    #[clap(long = "value", short = 'v')]
    pub val: Option<String>,
}

#[derive(Debug, Args)]
#[command(
    subcommand_value_name = "resource",
    subcommand_help_heading = "Resources"
)]
pub struct DeleteArgs {
    #[clap(subcommand)]
    pub resource: DeleteCommand,
}

/// For data integrity and safety, we provide no method of deleting
/// iterations, params, samples, periods, metric_desc's, or metric_data's.
/// This should generally be unnecessary as the will automatically be
/// removed when their parent resource is deleted.
#[derive(Debug, Subcommand)]
pub enum DeleteCommand {
    Run(DeleteRunArgs),
    Tag(DeleteTagArgs),
}

#[derive(Debug, Args)]
pub struct DeleteRunArgs {
    #[clap(long = "run-uuid", short = 'u')]
    pub run_uuid: Option<Uuid>,
    /// Delete for runs where "tag_name=tag_value"
    #[clap(long = "tag", short = 't')]
    pub tag: Option<String>,
    /// Delete for runs that begin before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-before", short = 'b', value_parser = parse_timestamp)]
    pub begin_before: Option<DateTime<Utc>>,
    /// Delete for runs that begin after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin-after", value_parser = parse_timestamp)]
    pub begin_after: Option<DateTime<Utc>>,
    /// Delete for runs that finish before this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-before", short = 'f', value_parser = parse_timestamp)]
    pub finish_before: Option<DateTime<Utc>>,
    /// Delete for runs that finish after this time.
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish-after", value_parser = parse_timestamp)]
    pub finish_after: Option<DateTime<Utc>>,
    #[clap(long = "benchmark", short = 'k')]
    pub benchmark: Option<String>,
    #[clap(long = "email", short = 'e')]
    pub email: Option<String>,
    #[clap(long = "name", short = 'n')]
    pub name: Option<String>,
    #[clap(long = "source", short = 's')]
    pub source: Option<String>,
}

#[derive(Debug, Args)]
pub struct DeleteTagArgs {
    #[clap(long = "run-uuid", short = 'r')]
    pub run_uuid: Option<Uuid>,
    /// Delete for tags where "tag_name=tag_value"
    #[clap(long = "tag", short = 't')]
    pub tag: Option<String>,
}

#[derive(Debug, Args)]
pub struct MetricArgs {
    #[clap(long = "run-uuid", short = 'r')]
    pub run_uuid: Option<Uuid>,
    #[clap(long = "iteration-uuid", short = 'i')]
    pub iteration_uuid: Option<Uuid>,
    #[clap(long = "metric-desc-uuid", short = 'm')]
    pub metric_desc_uuid: Option<Uuid>,
    /// Restricts the considered data to data that belonds to this period.
    /// This is notably different than the `ref-period` option.
    #[clap(long = "period-uuid", short = 'p')]
    pub period_uuid: Option<Uuid>,
    #[clap(long = "metric-type", short = 't')]
    pub metric_type: Option<String>,

    /// ref-period is a convenience option to use in place of specifying both a `begin`,
    /// and an `end`. It inherits the period's begin and end.
    #[clap(long = "ref-period", conflicts_with_all = ["begin", "finish"])]
    pub ref_period: Option<Uuid>,
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "begin", short = 'b', value_parser = parse_timestamp, conflicts_with = "ref_period", requires = "finish")]
    pub begin: Option<DateTime<Utc>>,
    /// Either a Unix epoch timestamp in millis, or a valid RFC 3339 timestamp
    #[clap(long = "finish", short = 'f', value_parser = parse_timestamp, conflicts_with = "ref_period", requires = "begin")]
    pub finish: Option<DateTime<Utc>>,

    #[clap(long = "resolution", default_value_t = 1)]
    pub resolution: u64,

    #[clap(long = "value-eq")]
    /// Search for values equal to
    pub value_eq: Option<f64>,
    #[clap(long = "value-lt")]
    /// Search for values less than
    pub value_lt: Option<f64>,
    /// Search for values greater than
    #[clap(long = "value-gt")]
    pub value_gt: Option<f64>,
    /// Names used to breakout the data. Provide a comma separated list of
    /// names, with or without a corresponding value. Ex: "hostname,userenv=fedora40"
    #[clap(long = "name", short = 'n', value_delimiter = ',')]
    pub name: Option<Vec<String>>,
    #[clap(value_enum, long = "aggregator", short = 'a', requires = "name", default_value_t = Aggregator::None)]
    pub aggregator: Aggregator,

    #[clap(long = "output", short = 'o')]
    pub output: Option<OutputFormat>,
}

#[derive(Debug, ValueEnum, Clone)]
pub enum Aggregator {
    None,
    Avg,
    WeightedAvg,
    Stddev,
    Min,
    Max,
}
