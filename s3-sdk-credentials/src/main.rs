use aws_types::credentials::*;
use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let bucket_name = "datafusion-parquet-testing";

    let sdk_config = aws_config::load_from_env().await;
    let credentials_providder = sdk_config
        .credentials_provider()
        .expect("could not find credentials provider");
    let credentials = credentials_providder
        .provide_credentials()
        .await
        .expect("could not load credentials");

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(
            sdk_config
                .region()
                .expect("could not find region")
                .to_string(),
        )
        .with_access_key_id(credentials.access_key_id())
        .with_secret_access_key(credentials.secret_access_key())
        .with_token(
            credentials
                .session_token()
                .expect("could not find session_token")
                .to_string(),
        )
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
