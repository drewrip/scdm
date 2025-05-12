use crate::SCDMError;
use crate::cdm;
use anyhow::Result;
use sqlx::postgres::PgPool;
use std::error::Error;

pub fn merr<T: Error>(err: T) -> SCDMError {
    SCDMError::FailedTableInit(err.to_string())
}

pub async fn init_tables(pool: &PgPool) -> Result<()> {
    let mut txn = pool.begin().await.map_err(merr)?;
    // Creation order is important here because of foreign keys.
    // The other option is to defer the integrity check until the
    // transaction commits.
    sqlx::query(cdm::SQL_TABLE_RUN)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_TAG)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_ITERATION)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_PARAM)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_SAMPLE)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_PERIOD)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_METRIC_DESC)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_NAME)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    sqlx::query(cdm::SQL_TABLE_METRIC_DATA)
        .execute(&mut *txn)
        .await
        .map_err(merr)?;
    txn.commit().await.map_err(merr)?;

    Ok(())
}
