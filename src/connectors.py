import csv
import json
import sqlite3
from pathlib import Path
from urllib import error, request


TRANSFORMATIONS = [
    {
        "type": "rename_fields",
        "name": "Rename fields",
        "description": "Renames keys using a mapping.",
    },
    {
        "type": "select_fields",
        "name": "Select fields",
        "description": "Keeps only the selected fields.",
    },
    {
        "type": "filter_equals",
        "name": "Filter equals",
        "description": "Keeps only records whose field equals a value.",
    },
    {
        "type": "add_fields",
        "name": "Add fields",
        "description": "Adds constant fields to every record.",
    },
    {
        "type": "uppercase_fields",
        "name": "Uppercase fields",
        "description": "Uppercases string values for the selected fields.",
    },
]


SOURCE_CONNECTORS = {
    "inline_json": {
        "type": "inline_json",
        "name": "Inline JSON",
        "description": "Use JSON records embedded directly in the pipeline definition.",
    },
    "csv_file": {
        "type": "csv_file",
        "name": "CSV file",
        "description": "Read records from a CSV file stored inside the platform data directory.",
    },
    "http_json": {
        "type": "http_json",
        "name": "HTTP JSON",
        "description": "Fetch JSON records from an HTTP endpoint.",
    },
}


DESTINATION_CONNECTORS = {
    "jsonl_file": {
        "type": "jsonl_file",
        "name": "JSONL file",
        "description": "Write newline-delimited JSON into the platform data directory.",
    },
    "sqlite_file": {
        "type": "sqlite_file",
        "name": "SQLite file",
        "description": "Load records into a SQLite table inside the platform data directory.",
    },
}


def catalog():
    return {
        "sources": list(SOURCE_CONNECTORS.values()),
        "transformations": TRANSFORMATIONS,
        "destinations": list(DESTINATION_CONNECTORS.values()),
    }


SOURCE_SPECS = {
    "inline_json": {
        "type": "object",
        "required": ["records"],
        "properties": {
            "records": {
                "type": "array",
                "items": {"type": "object"},
            }
        },
    },
    "csv_file": {
        "type": "object",
        "required": ["path"],
        "properties": {
            "path": {"type": "string"},
            "delimiter": {"type": "string", "default": ","},
            "has_header": {"type": "boolean", "default": True},
            "field_names": {"type": "array", "items": {"type": "string"}},
            "encoding": {"type": "string", "default": "utf-8"},
        },
    },
    "http_json": {
        "type": "object",
        "required": ["url"],
        "properties": {
            "url": {"type": "string"},
            "records_key": {"type": "string"},
            "timeout_seconds": {"type": "integer", "default": 10},
        },
    },
}


DESTINATION_SPECS = {
    "jsonl_file": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
        },
    },
    "sqlite_file": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "table": {"type": "string"},
            "mode": {"type": "string", "enum": ["replace", "append"], "default": "replace"},
        },
    },
}


def source_spec(name):
    return SOURCE_SPECS[name]


def destination_spec(name):
    return DESTINATION_SPECS[name]


def source_check(name, config, platform):
    if name == "inline_json":
        records = config.get("records")
        ok = isinstance(records, list) and all(isinstance(record, dict) for record in records)
        return {"ok": ok, "message": "records are valid" if ok else "records must be a list of objects"}

    if name == "csv_file":
        try:
            source_path = platform.resolve_storage_path(config.get("path"))
            platform.ensure_safe_existing_file(source_path)
            return {"ok": True, "message": f"csv source available at {source_path}"}
        except Exception as exc:  # pragma: no cover - defensive
            return {"ok": False, "message": str(exc)}

    if name == "http_json":
        try:
            _fetch_http_json(config)
            return {"ok": True, "message": "http source reachable"}
        except Exception as exc:  # pragma: no cover - defensive
            return {"ok": False, "message": str(exc)}

    raise KeyError(name)


def destination_check(name, config, platform):
    try:
        if name == "jsonl_file":
            platform.resolve_storage_path(config.get("path"), "exports/check.jsonl")
            return {"ok": True, "message": "jsonl destination is valid"}
        if name == "sqlite_file":
            target = platform.resolve_storage_path(config.get("path"), "warehouse/check.db")
            if not str(config.get("table") or "records").strip():
                return {"ok": False, "message": "table must be non-empty"}
            return {"ok": True, "message": f"sqlite destination is valid at {target}"}
    except Exception as exc:  # pragma: no cover - defensive
        return {"ok": False, "message": str(exc)}
    raise KeyError(name)


def source_discover(name, config, platform):
    if name == "inline_json":
        records = config.get("records", [])
        fields = sorted({key for record in records for key in record.keys()})
        return {"streams": [{"name": "inline_records", "fields": fields}]}

    if name == "csv_file":
        source_path = platform.resolve_storage_path(config.get("path"))
        platform.ensure_safe_existing_file(source_path)
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        has_header = config.get("has_header", True)
        field_names = config.get("field_names")
        with source_path.open("r", encoding=encoding, newline="") as handle:
            if has_header:
                reader = csv.DictReader(handle, delimiter=delimiter)
                fields = list(reader.fieldnames or [])
            else:
                if not field_names:
                    raise ValueError("csv_file without headers requires field_names")
                fields = list(field_names)
        return {"streams": [{"name": source_path.stem, "fields": fields}]}

    if name == "http_json":
        records = read_source(name, config, platform)
        fields = sorted({key for record in records for key in record.keys()})
        stream_name = Path(config.get("records_key") or "records").name
        return {"streams": [{"name": stream_name, "fields": fields}]}

    raise KeyError(name)


def read_source(name, config, platform):
    if name == "inline_json":
        return [dict(record) for record in config["records"]]

    if name == "csv_file":
        source_path = platform.resolve_storage_path(config.get("path"))
        platform.ensure_safe_existing_file(source_path)
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        has_header = config.get("has_header", True)
        field_names = config.get("field_names")
        with source_path.open("r", encoding=encoding, newline="") as handle:
            if has_header:
                reader = csv.DictReader(handle, delimiter=delimiter)
                return [dict(row) for row in reader]
            if not field_names:
                raise ValueError("csv_file without headers requires field_names")
            reader = csv.reader(handle, delimiter=delimiter)
            return [dict(zip(field_names, row)) for row in reader]

    if name == "http_json":
        payload = _fetch_http_json(config)
        records_key = config.get("records_key")
        if records_key:
            payload = payload.get(records_key)
        elif isinstance(payload, dict) and "records" in payload:
            payload = payload["records"]
        if not isinstance(payload, list) or any(not isinstance(record, dict) for record in payload):
            raise ValueError("http_json source must resolve to a list of objects")
        return payload

    raise KeyError(name)


def _fetch_http_json(config):
    with request.urlopen(config["url"], timeout=config.get("timeout_seconds", 10)) as response:
        return json.loads(response.read().decode("utf-8"))


def write_destination(name, config, records, platform, pipeline):
    if name == "jsonl_file":
        default_name = f"exports/{pipeline['id']}.jsonl"
        output_path = platform.resolve_storage_path(config.get("path"), default_name)
        platform.ensure_safe_creatable_file(output_path)
        with platform.open_output_handle(output_path) as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")
        return {"type": "jsonl_file", "path": str(output_path), "row_count": len(records)}

    if name == "sqlite_file":
        default_name = f"warehouse/{pipeline['id']}.db"
        output_path = platform.resolve_storage_path(config.get("path"), default_name)
        platform.ensure_safe_creatable_file(output_path)
        table = config.get("table") or platform.slugify(pipeline["name"])
        mode = config.get("mode", "replace")
        _load_sqlite(output_path, table, mode, records, platform)
        return {
            "type": "sqlite_file",
            "path": str(output_path),
            "table": table,
            "mode": mode,
            "row_count": len(records),
        }

    raise KeyError(name)


def _load_sqlite(output_path, table_name, mode, records, platform):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    platform.ensure_safe_creatable_file(output_path)
    connection = sqlite3.connect(output_path)
    try:
        cursor = connection.cursor()
        quoted_table = platform.quote_identifier(table_name)
        if mode == "replace":
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        columns = sorted({key for record in records for key in record.keys()})
        if not columns:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} (_raw_json TEXT)")
            connection.commit()
            return
        column_sql = []
        for column in columns:
            values = [record.get(column) for record in records if record.get(column) is not None]
            column_sql.append(f"{platform.quote_identifier(column)} {platform.infer_sqlite_type(values)}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({', '.join(column_sql)})")
        if records:
            placeholders = ", ".join("?" for _ in columns)
            insert_columns = ", ".join(platform.quote_identifier(column) for column in columns)
            rows = [tuple(platform.sqlite_value(record.get(column)) for column in columns) for record in records]
            cursor.executemany(
                f"INSERT INTO {quoted_table} ({insert_columns}) VALUES ({placeholders})",
                rows,
            )
        connection.commit()
    finally:
        connection.close()
