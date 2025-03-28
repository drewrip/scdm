/// SQL table definitions that match the OpenSearch CDM indices
pub const SQL_TABLE_ITERATION: &str = r#"
    CREATE TABLE IF NOT EXISTS iteration (
        iteration_uuid uuid PRIMARY KEY,
        num bigint,
        status text,
        path text,
        primary_metric text,
        primary_period text
    )
"#;

pub const SQL_TABLE_METRIC_DATA: &str = r#"
    CREATE TABLE IF NOT EXISTS metric_data (
        metric_data_id bigserial PRIMARY KEY,
        metric_desc_uuid uuid,
        value double precision,
        begin timestamp,
        finish timestamp,
        duration bigint,
        CONSTRAINT fk_metric_desc FOREIGN KEY (metric_desc_uuid) REFERENCES metric_desc(metric_desc_uuid)
    )
"#;

pub const SQL_TABLE_METRIC_DESC: &str = r#"
    CREATE TABLE IF NOT EXISTS metric_desc (
        metric_desc_uuid uuid PRIMARY KEY,
        class text,
        type text,
        source text,
        names_list text,
        names text,
        value_format text,
        values text
    )
"#;

/// Much of this is WIP, not sure what these fields type should be
pub const SQL_TABLE_PARAM: &str = r#"
    CREATE TABLE IF NOT EXISTS param (
        param_uuid uuid PRIMARY KEY,
        id text,
        arg text,
        role text,
        val text
    )
"#;

pub const SQL_TABLE_PERIOD: &str = r#"
    CREATE TABLE IF NOT EXISTS period (
        period_uuid uuid PRIMARY KEY,
        begin timestamp,
        finish timestamp,
        name text,
        prev_id uuid
    )
"#;

pub const SQL_TABLE_RUN: &str = r#"
    CREATE TABLE IF NOT EXISTS run (
        run_uuid uuid PRIMARY KEY,
        begin timestamp,
        finish timestamp,
        harness text,
        benchmark text,
        host text,
        email text,
        name text,
        description text,
        tags text,
        source text
    )
"#;

pub const SQL_TABLE_SAMPLE: &str = r#"
    CREATE TABLE IF NOT EXISTS sample (
        sample_uuid uuid PRIMARY KEY,
        num bigint,
        status text,
        path text
    )
"#;

pub const SQL_TABLE_TAG: &str = r#"
    CREATE TABLE IF NOT EXISTS tag (
        tag_uuid uuid PRIMARY KEY,
        name text,
        val text
    )
"#;
