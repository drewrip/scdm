use anyhow::Result;
use sqlx::PgPool;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Couldn't find path, or it isn't a directory: {0}")]
    InvalidPath(String),
}

pub async fn parse(pool: &PgPool, dir_path: &Path) -> Result<()> {
    let res = sqlx::query!("SELECT (1) as c").fetch_all(pool).await?;
    println!("{:?}", res);
    Ok(())
}
