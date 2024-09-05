# Meilisearch Importer

The most efficient CLI tool to import massive CSVs, NDJSON, or JSON (array of objects) into Meilisearch.

This tool has been tested with datasets ranging from hundreds of thousands to over forty million documents. The progress bar is particularly useful for monitoring large imports.

## Features

- Uploads millions of documents to Meilisearch
- Automatically retries on error with exponential backoff
- Shows upload progress with estimated time of arrival (ETA)
- Works with [Meilisearch Cloud](https://www.meilisearch.com/cloud) and self-hosted instances
- Supports CSV, NDJSON, and JSON file formats
- Configurable batch size for optimized imports
- Optional CSV delimiter specification
- Ability to skip batches for resuming interrupted imports
- Support for both "add or replace" and "add or update" operations

## Installation

Download the latest version of this tool from the [releases page](https://github.com/meilisearch/meilisearch-importer/releases).

## Command-line Options

- `--url`: Meilisearch instance URL (required)
- `--index`: Index name to send documents to (required)
- `--files`: List of file paths to import (required, supports multiple files)
- `--primary-key`: Field to use as the primary key
- `--api-key`: API key for authentication
- `--batch-size`: Size of document batches (default: 20 MiB)
- `--csv-delimiter`: Custom delimiter for CSV files
- `--skip-batches`: Number of batches to skip (for resuming imports)
- `--upload-operation`: Choose between `add-or-replace` (default) and `add-or-update`


## Usage Examples

### Import to Meilisearch Cloud

```bash
meilisearch-importer \
--url 'https://ms-************.sfo.meilisearch.io' \
--index products \
--primary-key uuid \
--api-key 'D2jkS***************' \
--files products.csv
```

### Import Large CSV File with Custom Delimiter

```bash
meilisearch-importer \
--url 'https://ms-************.sfo.meilisearch.io' \
--api-key 'D2jkS***************' \
--index products \
--primary-key uuid \
--files large_product_list.csv \
--csv-delimiter ';' \
--batch-size 50MB
```

### Import Multiple Files

```bash
meilisearch-importer \
--url 'https://ms-************.sfo.meilisearch.io' \
--api-key 'D2jkS***************' \
--index library \
--files books.json authors.json publishers.ndjson
```

### Use Add or Update Operation

```bash
meilisearch-importer \
--url 'https://ms-************.sfo.meilisearch.io' \
--api-key 'D2jkS***************' \
--index users \
--files users_update.json \
--upload-operation add-or-update
```

### Resume Interrupted Import

```bash
meilisearch-importer \
--url 'https://ms-************.sfo.meilisearch.io' \
--api-key 'D2jkS***************' \
--index large_dataset \
--files huge_file.ndjson \
--skip-batches 100
```

## Error Handling and Retries

The importer automatically retries on errors using an exponential backoff strategy:
- Starts with a 100ms delay
- Increases delay up to a maximum of 1 hour
- Makes up to 20 retry attempts before giving up

## Supported File Formats

- JSON: Must contain an array of objects
- NDJSON: Each line should be a valid JSON object
- CSV: Can specify custom delimiters with `--csv-delimiter`

## Troubleshooting

- "Too many errors": Check your network connection and Meilisearch instance status.
- "File does not exist": Verify file paths and permissions.
- "Failed to read CSV headers": Ensure your CSV file is properly formatted and uses the correct delimiter.
- If uploads are slow, try increasing the `--batch-size` or check your network speed.

## Contributing

We welcome contributions to the Meilisearch Importer! Please check out our [Contributing Guide](CONTRIBUTING.md) for more information on how to get started.

## License

Meilisearch Importer is released under the MIT License. See the [LICENSE](LICENSE) file for details.