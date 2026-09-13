# etl

Airbyte-inspired end-to-end ETL platform built with the Python standard library.

## Features

- browser UI for creating, previewing, running, and deleting pipelines
- JSON API for pipeline, file, run, dashboard, and connector management
- source connectors:
  - `inline_json`
  - `csv_file`
  - `http_json`
- transformations:
  - `rename_fields`
  - `select_fields`
  - `filter_equals`
  - `add_fields`
  - `uppercase_fields`
- destinations:
  - `jsonl_file`
  - `sqlite_file`
- scheduled execution with persisted run history and logs
- built-in demo HTTP source at `/api/demo/contacts`
- safe file handling that keeps reads and writes inside the platform data directory

## Run locally

```bash
python app.py
```

Then open `http://127.0.0.1:8000`.

## Test

```bash
python -m unittest discover -s tests -v
```

## Complete browser workflow

1. Start the server.
2. Open the UI.
3. Keep the default `http_json` source and `sqlite_file` destination.
4. Click **Create pipeline**.
5. Click **Run now** on the new pipeline.
6. Inspect the run logs in the UI.
7. The loaded SQLite database will be written under `data/warehouse/`.

## Example API payload

```json
{
  "name": "demo-http-sync",
  "enabled": true,
  "schedule_interval_seconds": 300,
  "source": {
    "type": "http_json",
    "config": {
      "url": "http://127.0.0.1:8000/api/demo/contacts",
      "records_key": "records",
      "timeout_seconds": 10
    }
  },
  "transformations": [
    {
      "type": "rename_fields",
      "config": {
        "mapping": {
          "email": "email_address"
        }
      }
    },
    {
      "type": "uppercase_fields",
      "config": {
        "fields": ["country"]
      }
    },
    {
      "type": "add_fields",
      "config": {
        "values": {
          "synced_by": "etl-platform"
        }
      }
    }
  ],
  "destination": {
    "type": "sqlite_file",
    "config": {
      "path": "warehouse/contacts.db",
      "table": "contacts",
      "mode": "replace"
    }
  }
}
```

## CSV workflow

You can create a CSV source file through the API first:

```json
POST /api/files
{
  "path": "uploads/customers.csv",
  "content": "id,name,email\n1,Ada,ada@example.com\n2,Grace,grace@example.com\n"
}
```

Then create a pipeline using:

```json
{
  "source": {
    "type": "csv_file",
    "config": {
      "path": "uploads/customers.csv",
      "has_header": true
    }
  }
}
```
