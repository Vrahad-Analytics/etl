# etl

Airbyte-inspired ETL MVP built with the Python standard library.

## What it does

- creates ETL pipelines through a JSON API
- provides a small browser UI to create and run pipelines
- extracts records from an inline JSON source
- applies simple transformations (`rename_fields`, `select_fields`)
- loads results into a JSONL file destination
- stores pipeline and run history on disk

## Run locally

```bash
python app.py
```

Then open `http://127.0.0.1:8000`.

## Test

```bash
python -m unittest discover -s tests -v
```

## Example API payload

```json
{
  "name": "contacts-sync",
  "source": {
    "type": "inline_json",
    "config": {
      "records": [
        { "id": 1, "name": "Ada", "email": "ada@example.com" }
      ]
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
      "type": "select_fields",
      "config": {
        "fields": ["id", "name", "email_address"]
      }
    }
  ],
  "destination": {
    "type": "jsonl_file",
    "config": {
      "path": "data/contacts-sync.jsonl"
    }
  }
}
```
