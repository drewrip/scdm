use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool};
use std::env;
use std::path::Path;
use thiserror::Error;

pub mod cdm;
pub mod parser;
pub mod query;

#[derive(Debug, Parser)]
#[clap(name = "scdm", version)]
pub struct App {
    #[clap(flatten)]
    global_opts: GlobalOpts,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct GlobalOpts {
    /// Verbosity level (can be specified multiple times)
    #[clap(long, short = 'v', action)]
    verbose: bool,

    /// The DB_USER Env variable takes precedence
    #[clap(long = "db-user", short = 'u')]
    db_user: Option<String>,

    /// The DB_PASSWORD Env variable takes precedence
    #[clap(long = "db-password", short = 'p')]
    db_password: Option<String>,

    /// The DB_URL Env variable takes precedence
    #[clap(long = "db-url")]
    db_url: Option<String>,

    /// The DB_PORT Env variable takes precedence
    #[clap(long = "db-port")]
    db_port: Option<String>,

    /// The DB_NAME Env variable takes precedence
    #[clap(long = "db-name", default_value = "scdm")]
    db_name: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Parse the results of a crucible iteration and import into DB
    Parse(ParseArgs),
    /// Query the the CDM DB
    Query(QueryArgs),
}

#[derive(Debug, Args)]
struct ParseArgs {
    path: String,
}

#[derive(Debug, Args)]
struct QueryArgs {}

#[derive(Error, Debug)]
pub enum SCDMError {
    #[error("Couldn't find DB login info as an ENV variable or cli arg: {0}")]
    MissingDBInfo(String),
    #[error("Invalid DB login info provided: {0}")]
    InvalidDBInfo(String),
    #[error("Failed to create the necessary tables: {0}")]
    FailedTableInit(String),
}

pub async fn build_tables(pool: &PgPool) -> Result<()> {
    sqlx::query(cdm::SQL_TABLE_ITERATION).execute(pool).await?;
    sqlx::query(cdm::SQL_TABLE_METRIC_DESC)
        .execute(pool)
        .await?;
    sqlx::query(cdm::SQL_TABLE_METRIC_DATA)
        .execute(pool)
        .await?;
    sqlx::query(cdm::SQL_TABLE_PARAM).execute(pool).await?;
    sqlx::query(cdm::SQL_TABLE_PERIOD).execute(pool).await?;
    sqlx::query(cdm::SQL_TABLE_RUN).execute(pool).await?;
    sqlx::query(cdm::SQL_TABLE_SAMPLE).execute(pool).await?;
    sqlx::query(cdm::SQL_TABLE_TAG).execute(pool).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = App::parse();

    let db_user = env::var("DB_USER").or(args
        .global_opts
        .db_user
        .ok_or(SCDMError::MissingDBInfo(String::from("DB_USER"))))?;
    let db_password = env::var("DB_PASSWORD").or(args
        .global_opts
        .db_password
        .ok_or(SCDMError::MissingDBInfo(String::from("DB_PASSWORD"))))?;
    let db_url = env::var("DB_URL").or(args
        .global_opts
        .db_url
        .ok_or(SCDMError::MissingDBInfo(String::from("DB_URL"))))?;

    let db_port: u16 = env::var("DB_PORT")
        .unwrap_or(args.global_opts.db_port.unwrap_or(String::from("5432")))
        .parse::<u16>()
        .map_err(|e| {
            SCDMError::InvalidDBInfo(format!("Couldn't convert provided port to a u16 ({})", e))
        })?;

    let db_name = env::var("DB_NAME").or(args.global_opts.db_name.ok_or(
        SCDMError::InvalidDBInfo(String::from("No database name provided")),
    ))?;

    let conn_opts = PgConnectOptions::new()
        .host(&db_url)
        .port(db_port)
        .database(&db_name)
        .username(&db_user)
        .password(&db_password);

    let pool = PgPool::connect_with(conn_opts).await?;

    build_tables(&pool)
        .await
        .map_err(|e| SCDMError::FailedTableInit(format!("failure {}", e)))?;

    let result = match args.command {
        Command::Parse(args) => {
            let dir_path = Path::new(&args.path);
            parser::parse(&pool, dir_path).await
        }
        Command::Query(args) => {
            todo!("Query not implemented yet");
            Ok(())
        }
    };

    result
}
