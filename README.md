# Meilisearch Importer

The most efficient CLI tool to import massive CSVs, NSJSON or JSON (array of objects) into Meilisearch.

This tool has been tested with multiple datasets from hundreds of thousand documents to some with more than forty millions documents. The progress bar is very handy in this case.

## Features

 - Uploads millions of documents to Meilisearch.
 - Automatically retries on error.
 - Shows the upload progress along with the estimated time of arrival (ETA).
 - [Works on the Cloud](https://www.meilisearch.com/pricing) and on self-hosted instances.

## Installation

You can download the latest version of this tool [on the release page](https://github.com/meilisearch/meilisearch-importer/releases).

## Example Usage

### Send Documents to the Cloud

It's straightforward to [create a project on the Cloud](https://www.meilisearch.com/pricing) and send your documents into it.

If you cannot send your dataset directly from the website by drag-and-dropping it, this tool is perfect for you. You can send them by running the following command:

```bash
meilisearch-importer \
    --url 'https://ms-************.sfo.meilisearch.io'
    --index crunchbase \
    --primary-key uuid \
    --token 'D2jkS***************' \
    --files ./dataset/organizations.csv
```

### Send Documents to a Local Instance

This tool is also useful when you want to test Meilisearch locally. The only mandatory parameters to define are the URL, the index name and your dataset.

However, you can also increase the batch size to make meilisearch index faster.

```bash
meilisearch-importer \
    --url 'http://localhost:7700'
    --index movies \
    --files movies.json \
    --batch-size 100MB
```
