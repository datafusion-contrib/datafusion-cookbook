use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreUrl;
use futures::{StreamExt, TryStreamExt};
use futures::stream::BoxStream;
use glob::Pattern;
use object_store::ObjectMeta;
use object_store::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let bucket_name = "datafusion-parquet-testing";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(env::var("AWS_REGION").unwrap_or("eu-central-1".to_string()))
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .build()?;

    ctx.runtime_env()
        .register_object_store("s3", bucket_name, Arc::new(s3));

    let bucket = format!("s3://{}", bucket_name);
    let bucket_url = ObjectStoreUrl::parse(&bucket).expect("failed to parse s3 url");

    //let filename = format!("s3://{}/data/alltypes_pl*n.parquet", bucket_name);

    let prefix_path = Path::parse("data").expect("failed to parse path");
    let glob = Pattern::new("data/alltypes_pl*n.parquet").expect("failed to create glob pattern");

    let store = ctx.runtime_env().object_store(bucket_url)?;
    println!("store: {}", store);

    let list_result = store.list(Some(&prefix_path)).await.expect("failed to find files");
    let matching_files_result: BoxStream<Result<ObjectMeta>> = list_result
        .map_err(Into::into)
        .try_filter(move |meta| {
            let ok = glob.matches(meta.location.as_ref());
            futures::future::ready(ok)
        })
        .boxed();

    let matching_files: Vec<ObjectMeta> = matching_files_result.try_collect().await?;
    println!("matching files: {:?}", matching_files);

    let matching_file_urls: Vec<_> = matching_files
        .iter()
        .map(|meta| {
            ListingTableUrl::parse(format!("{}/{}", bucket, meta.location.as_ref())).expect("failed to create listingtableurl")
        })
        .collect();
    println!("matching_file_urls: {:?}", matching_file_urls);

    let mut config = ListingTableConfig::new_with_multi_paths(matching_file_urls);
    config = config.infer_options(&ctx.state.read()).await?;
    config = config.infer_schema(&ctx.state.read()).await?;

    let provider = ListingTable::try_new(config)?;

    let df = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    df.show().await?;

    Ok(())
}
