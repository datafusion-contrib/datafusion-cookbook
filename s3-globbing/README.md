# Query multiple files on S3 with a globbing pattern

The current ListingTableUrl implementation has some limitations:
* does not support globbing patterns in an S3 url
* has issues/difficulties with format and schema inference (it does not exclude "hidden" folders and files)

In this example we glob files (eg: s3://bucket/nyc-taxi/green-*.csv) build retrieving the actual files and then composing a ListingTableConfig with multiple paths.