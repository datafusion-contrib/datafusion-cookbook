use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::env;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let bucket_name = "datafusion-parquet-testing";
    let globbing_path = format!("s3://{}/data/alltypes_pl*n.parquet", bucket_name);

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(env::var("AWS_REGION").unwrap_or_else(|_| "eu-central-1".to_string()))
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .build()?;

    ctx.runtime_env()
        .register_object_store("s3", bucket_name, Arc::new(s3));

    let matching_files = list_matching_files(&ctx, &globbing_path).await?;

    let matching_file_urls: Vec<_> = matching_files
        .iter()
        .map(|meta| {
            ListingTableUrl::parse(format!("s3://{}/{}", bucket_name, meta.location.as_ref()))
                .expect("failed to create listingtableurl")
        })
        .collect();

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

async fn list_matching_files(ctx: &SessionContext, globbing_path: &str) -> Result<Vec<ObjectMeta>> {
    let (object_store_url, path) = extract_object_store_url_and_path(globbing_path)?;
    let prefix_path = extract_leading_path_without_glob_characters(&path);
    let glob = Pattern::new(&path).map_err(|_| {
        DataFusionError::Execution(format!("Failed to create globbing pattern from {}", &path))
    })?;

    let store = ctx.runtime_env().object_store(&object_store_url)?;

    let list_result = store.list(Some(&prefix_path)).await?;

    let matching_files_result: BoxStream<Result<ObjectMeta>> = list_result
        .map_err(Into::into)
        .try_filter(move |meta| {
            let ok = glob.matches(meta.location.as_ref());
            futures::future::ready(ok)
        })
        .boxed();

    let matching_files: Vec<ObjectMeta> = matching_files_result.try_collect().await?;

    Ok(matching_files)
}

fn extract_object_store_url_and_path(globbing_path: &str) -> Result<(ObjectStoreUrl, String)> {
    let url = Url::parse(globbing_path).map_err(|_| {
        DataFusionError::Execution(format!("Failed to parse {} as url.", &globbing_path))
    })?;
    let bucket = &url[..url::Position::BeforePath];
    let bucket_url = ObjectStoreUrl::parse(&bucket)?;
    let path = url
        .path()
        .strip_prefix(object_store::path::DELIMITER)
        .unwrap_or_else(|| url.path());
    Ok((bucket_url, String::from(path)))
}

#[test]
fn test_extract_object_store_url_and_path() {
    let actual = extract_object_store_url_and_path("s3://bucket").unwrap();
    assert_eq!(("s3://bucket/", ""), (actual.0.as_str(), actual.1.as_str()));

    let actual = extract_object_store_url_and_path("s3://bucket/").unwrap();
    assert_eq!(("s3://bucket/", ""), (actual.0.as_str(), actual.1.as_str()));

    let actual = extract_object_store_url_and_path("s3://bucket/path").unwrap();
    assert_eq!(
        ("s3://bucket/", "path"),
        (actual.0.as_str(), actual.1.as_str())
    );
}

fn extract_leading_path_without_glob_characters(path: &str) -> Path {
    let leading_path_parts_without_glob: Vec<_> = path
        .split(object_store::path::DELIMITER)
        .take_while(|part| !part.contains('?') && !part.contains('*') && !part.contains('['))
        .collect();
    Path::from_iter(leading_path_parts_without_glob)
}

#[test]
fn test_extract_leading_path_without_glob_characters() {
    assert_eq!(
        "a/b/c",
        extract_leading_path_without_glob_characters("a/b/c").as_ref()
    );
    assert_eq!(
        "",
        extract_leading_path_without_glob_characters("a?").as_ref()
    );
    assert_eq!(
        "a/b",
        extract_leading_path_without_glob_characters("a/b/c*/d").as_ref()
    );
    assert_eq!(
        "a/b/c",
        extract_leading_path_without_glob_characters("a/b/c/d[ef].csv").as_ref()
    );
}
