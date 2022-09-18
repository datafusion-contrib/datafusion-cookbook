---
weight: 1
bookFlatSection: true
title: "Query multiple files on S3 with a globbing pattern"
---

# Query multiple files on S3 with a globbing pattern

The current ListingTableUrl implementation has some limitations:
* does not support globbing patterns in an S3 url
* has issues/difficulties with format and schema inference (it does not exclude "hidden" folders and files)

In this recipe we first fetch all matching files and then build a ListingTableConfig with multiple paths.