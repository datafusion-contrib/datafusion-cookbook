---
weight: 1
bookFlatSection: true
title: "Query a file on S3 with a custom endpoint (eg: minio)"
---

# Query a file on S3 with a custom endpoint (eg: minio)

Spin up [minio](https://min.io/) as a custom S3 endpoint:

```bash
docker run \
--detach \
--rm \
--publish 9000:9000 \
--publish 9001:9001 \
--name minio \
--volume "$(pwd)/..:/data" \
--env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
--env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
quay.io/minio/minio server /data \
--console-address ":9001"
```