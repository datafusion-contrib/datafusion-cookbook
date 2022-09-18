use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let bucket_name = "datafusion-parquet-testing";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region("eu-central-1")
        .with_access_key_id("AKIAIOSFODNN7EXAMPLE")
        .with_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        .with_endpoint("http://localhost:9000")
        .with_allow_http(true)
        .build()?;

    ctx.runtime_env()
        .register_object_store("s3", bucket_name, Arc::new(s3));

    let filename = format!("s3://{}/data/alltypes_plain.parquet", bucket_name);

    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    df.show().await?;

    Ok(())
}
