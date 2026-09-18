# etl

Airbyte-inspired local data integration platform with a more Airbyte-like shape: API server, worker, scheduler, connector lifecycle endpoints, job queue, browser UI, and Docker-based multi-process runtime.

## Architecture

- **API server**: serves the UI and JSON APIs, stores pipeline definitions, and enqueues jobs
- **Worker**: claims queued jobs and executes extract-transform-load runs
- **Scheduler**: scans enabled pipelines and enqueues scheduled jobs
- **Metadata store**: SQLite database at `data/metadata.db`
- **Shared data dir**: holds uploaded source files, exported JSONL files, and SQLite destination databases

## Connectors

### Source connectors
- `inline_json`
- `csv_file`
- `http_json`

### Destination connectors
- `jsonl_file`
- `sqlite_file`

### Transformations
- `rename_fields`
- `select_fields`
- `filter_equals`
- `add_fields`
- `uppercase_fields`

## Connector lifecycle APIs

- `GET /api/connectors`
- `GET /api/connectors/source/<name>/spec`
- `POST /api/connectors/source/<name>/check`
- `POST /api/connectors/source/<name>/discover`
- `GET /api/connectors/destination/<name>/spec`
- `POST /api/connectors/destination/<name>/check`

## Local run modes

Run all components in one process:

```bash
python app.py
```

Run only the API server:

```bash
python app.py api
```

Run only the worker:

```bash
python app.py worker
```

Run only the scheduler:

```bash
python app.py scheduler
```

Then open `http://127.0.0.1:8000`.

## Docker

Start separate API, worker, and scheduler services:

```bash
docker compose up --build
```

## Test

```bash
python -m unittest discover -s tests -v
```

## End-to-end browser workflow

1. Start the stack with `python app.py` or `docker compose up --build`.
2. Open the UI.
3. Keep the default `http_json` source pointing at `/api/demo/contacts`.
4. Keep the default `sqlite_file` destination.
5. Create the pipeline.
6. Click **Run now** to enqueue a job.
7. Watch the queued job move to running and then succeeded.
8. Inspect the loaded SQLite file under `data/warehouse/contacts.db`.

## Example pipeline payload

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
