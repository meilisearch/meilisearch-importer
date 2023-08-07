# Meilisearch Importer (Experimental)

A CLI to import massive CSV, NdJson or JSON (array of objects) into Meilisearch the most efficient way.

```
cargo run --release -- --url "/indexes/xxx/documents"  --token "xxx" xxx.csv
```

## Options

- `--url`: The URL of the instance (e.g., `--url https://xxx.meilisearch.io`).
- `--index`: The name of the index where you want to add the dataset (e.g., `--index products`).
- `--token`: The primary key or admin key of your project (e.g., `--token abc`).
- `--primary-key`: (Optional) The primary key of your index. The primary key will be automatically detected by default (e.g., `--primary-key uuid`).
- `--files`: A list of files or folders to index (e.g., `--files ./datasets/*`).
- `--batch-size`: The maximum size for a batch (e.g., `--batch-size 20MB`). Default 90MB.
