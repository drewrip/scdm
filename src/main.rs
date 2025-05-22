use anyhow::Result;
use args::Command;
use clap::Parser;
use sqlx::postgres::{PgConnectOptions, PgPool};
use std::env;
use std::path::Path;
use thiserror::Error;

pub mod add;
pub mod args;
pub mod cdm;
pub mod import;
pub mod init;
pub mod metric;
pub mod parser;
pub mod query;

#[derive(Error, Debug)]
pub enum SCDMError {
    #[error("Couldn't find DB login info as an ENV variable or cli arg: {0}")]
    MissingDBInfo(String),
    #[error("Invalid DB login info provided: {0}")]
    InvalidDBInfo(String),
    #[error("Failed to create the necessary tables: {0}")]
    FailedTableInit(String),
    #[error("Failed to parse timestamp: {0}")]
    FailedTimestampParse(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = args::App::parse();

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

    let result = match args.command {
        Command::Parse(parse_args) => {
            let dir_path = Path::new(&parse_args.path);
            parser::parse(&pool, dir_path).await
        }
        Command::Add(add_args) => {
            let path = Path::new(&add_args.path);
            add::add(&pool, path).await
        }
        Command::Query(query_args) => query::query(&pool, query_args).await,
        Command::Import(import_args) => import::import(&pool, import_args).await,
        Command::Init => init::init_tables(&pool).await,
    };

    result
}
