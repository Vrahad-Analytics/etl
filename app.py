import csv
import errno
import json
import os
import sqlite3
import threading
import time
import traceback
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error, request
from urllib.parse import urlparse


CONNECTORS = {
    "sources": [
        {
            "type": "inline_json",
            "name": "Inline JSON",
            "description": "Use JSON records embedded directly in the pipeline definition.",
        },
        {
            "type": "csv_file",
            "name": "CSV file",
            "description": "Read records from a CSV file stored inside the platform data directory.",
        },
        {
            "type": "http_json",
            "name": "HTTP JSON",
            "description": "Fetch JSON records from an HTTP endpoint.",
        },
    ],
    "transformations": [
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
    ],
    "destinations": [
        {
            "type": "jsonl_file",
            "name": "JSONL file",
            "description": "Writes newline-delimited JSON to a file in the data directory.",
        },
        {
            "type": "sqlite_file",
            "name": "SQLite file",
            "description": "Loads records into a SQLite database table in the data directory.",
        },
    ],
}


class PipelineExecutionError(Exception):
    pass


class PipelineBusyError(Exception):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def iso_to_epoch(value):
    if not value:
        return 0.0
    return datetime.fromisoformat(value).timestamp()


def slugify(value):
    letters = []
    for char in value.lower():
        if char.isalnum():
            letters.append(char)
        else:
            letters.append("_")
    result = "".join(letters).strip("_")
    while "__" in result:
        result = result.replace("__", "_")
    return result or "records"


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


class ETLPlatform:
    def __init__(self, state_path, scheduler_poll_interval=1.0):
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._state = self._load_state()
        self._active_pipeline_ids = set()
        self._scheduler_poll_interval = scheduler_poll_interval
        self._scheduler_stop = threading.Event()
        self._scheduler_thread = None

    @property
    def data_dir(self):
        return self.state_path.parent.resolve()

    def _load_state(self):
        if not self.state_path.exists():
            return {"pipelines": [], "runs": []}
        with self.state_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        data.setdefault("pipelines", [])
        data.setdefault("runs", [])
        return data

    def _save_state(self):
        temp_path = self.state_path.with_suffix(".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(self._state, handle, indent=2)
        temp_path.replace(self.state_path)

    def start_scheduler(self):
        with self._lock:
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                return
            self._scheduler_stop.clear()
            self._scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                name="etl-scheduler",
                daemon=True,
            )
            self._scheduler_thread.start()

    def stop_scheduler(self):
        self._scheduler_stop.set()
        thread = self._scheduler_thread
        if thread and thread.is_alive():
            thread.join(timeout=5)

    def scheduler_status(self):
        thread = self._scheduler_thread
        return {
            "running": bool(thread and thread.is_alive()),
            "poll_interval_seconds": self._scheduler_poll_interval,
        }

    def _scheduler_loop(self):
        while not self._scheduler_stop.is_set():
            self.run_scheduled_pipelines_once()
            self._scheduler_stop.wait(self._scheduler_poll_interval)

    def run_scheduled_pipelines_once(self, now_epoch=None):
        if now_epoch is None:
            now_epoch = time.time()
        due_pipeline_ids = []
        with self._lock:
            for pipeline in self._state["pipelines"]:
                interval = pipeline.get("schedule_interval_seconds")
                if not pipeline.get("enabled", True) or not interval:
                    continue
                if pipeline["id"] in self._active_pipeline_ids:
                    continue
                last_scheduled_run_at = pipeline.get("last_scheduled_run_at")
                if not last_scheduled_run_at:
                    due_pipeline_ids.append(pipeline["id"])
                    continue
                if now_epoch - iso_to_epoch(last_scheduled_run_at) >= interval:
                    due_pipeline_ids.append(pipeline["id"])

        runs = []
        for pipeline_id in due_pipeline_ids:
            try:
                runs.append(self.run_pipeline(pipeline_id, trigger="schedule"))
            except PipelineBusyError:
                continue
            except KeyError:
                continue
            except Exception:
                continue
        return runs

    def list_pipelines(self):
        with self._lock:
            return deepcopy(self._state["pipelines"])

    def get_pipeline(self, pipeline_id):
        with self._lock:
            for pipeline in self._state["pipelines"]:
                if pipeline["id"] == pipeline_id:
                    return deepcopy(pipeline)
        return None

    def delete_pipeline(self, pipeline_id):
        with self._lock:
            for index, pipeline in enumerate(self._state["pipelines"]):
                if pipeline["id"] == pipeline_id:
                    removed = self._state["pipelines"].pop(index)
                    self._save_state()
                    return removed
        return None

    def list_runs(self):
        with self._lock:
            return list(reversed(deepcopy(self._state["runs"])))

    def get_run(self, run_id):
        with self._lock:
            for run in self._state["runs"]:
                if run["id"] == run_id:
                    return deepcopy(run)
        return None

    def get_dashboard(self):
        with self._lock:
            pipelines = deepcopy(self._state["pipelines"])
            runs = deepcopy(self._state["runs"])
        latest_run = runs[-1] if runs else None
        return {
            "pipeline_count": len(pipelines),
            "enabled_pipeline_count": sum(1 for pipeline in pipelines if pipeline.get("enabled", True)),
            "run_count": len(runs),
            "latest_run": latest_run,
            "scheduler": self.scheduler_status(),
        }

    def create_pipeline(self, payload):
        pipeline = self._validate_pipeline(payload)
        now = utc_now()
        pipeline.update(
            {
                "id": f"pipe_{uuid.uuid4().hex[:12]}",
                "created_at": now,
                "updated_at": now,
                "last_run_at": None,
                "last_run_status": None,
                "last_scheduled_run_at": None,
            }
        )
        with self._lock:
            self._state["pipelines"].append(pipeline)
            self._save_state()
        return deepcopy(pipeline)

    def preview_pipeline(self, pipeline_id, limit=20):
        pipeline = self._get_pipeline_copy(pipeline_id)
        records = self._extract(pipeline["source"])
        records = self._transform(records, pipeline.get("transformations", []))
        return {
            "pipeline_id": pipeline_id,
            "record_count": len(records),
            "preview": records[:limit],
            "fields": sorted({key for record in records for key in record.keys()}),
        }

    def run_pipeline(self, pipeline_id, trigger="manual"):
        pipeline = self._get_pipeline_copy(pipeline_id)
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run = {
            "id": run_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline["name"],
            "trigger": trigger,
            "source_type": pipeline["source"]["type"],
            "destination_type": pipeline["destination"]["type"],
            "status": "running",
            "started_at": utc_now(),
            "finished_at": None,
            "record_count": 0,
            "preview": [],
            "output": None,
            "error": None,
            "logs": [],
        }

        with self._lock:
            if pipeline_id in self._active_pipeline_ids:
                raise PipelineBusyError("Pipeline is already running.")
            self._active_pipeline_ids.add(pipeline_id)
            self._state["runs"].append(deepcopy(run))
            self._save_state()

        try:
            self._append_run_log(run_id, f"Starting {trigger} run for {pipeline['name']}.")
            records = self._extract(pipeline["source"])
            self._append_run_log(run_id, f"Extracted {len(records)} record(s) from {pipeline['source']['type']}.")
            records = self._transform(records, pipeline.get("transformations", []))
            self._append_run_log(run_id, f"Transformed {len(records)} record(s).")
            output = self._load(records, pipeline)
            self._append_run_log(run_id, f"Loaded {len(records)} record(s) into {pipeline['destination']['type']}.")
            return self._finish_run_success(run_id, pipeline_id, records, output, trigger)
        except PipelineExecutionError as exc:
            return self._finish_run_failure(run_id, pipeline_id, str(exc), trigger)
        except Exception as exc:  # pragma: no cover - defensive guard
            self._append_run_log(run_id, traceback.format_exc())
            self._finish_run_failure(run_id, pipeline_id, f"Unexpected server error: {exc}", trigger)
            raise
        finally:
            with self._lock:
                self._active_pipeline_ids.discard(pipeline_id)

    def write_text_file(self, relative_path, content):
        if not isinstance(content, str):
            raise ValueError("File content must be a string.")
        output_path = self._resolve_storage_path(relative_path)
        self._ensure_real_parent_directories(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_real_parent_directories(output_path)
        try:
            with self._open_output_handle(output_path) as handle:
                handle.write(content)
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ValueError("File path must not use symlinks.") from exc
            raise
        return output_path

    def _get_pipeline_copy(self, pipeline_id):
        with self._lock:
            for pipeline in self._state["pipelines"]:
                if pipeline["id"] == pipeline_id:
                    return deepcopy(pipeline)
        raise KeyError(f"Unknown pipeline: {pipeline_id}")

    def _append_run_log(self, run_id, message):
        with self._lock:
            for run in self._state["runs"]:
                if run["id"] == run_id:
                    run["logs"].append(message)
                    self._save_state()
                    return

    def _finish_run_success(self, run_id, pipeline_id, records, output, trigger):
        with self._lock:
            completed_run = None
            for run in self._state["runs"]:
                if run["id"] == run_id:
                    run.update(
                        {
                            "status": "succeeded",
                            "finished_at": utc_now(),
                            "record_count": len(records),
                            "preview": records[:5],
                            "output": output,
                        }
                    )
                    completed_run = deepcopy(run)
                    break
            for pipeline in self._state["pipelines"]:
                if pipeline["id"] == pipeline_id:
                    pipeline["last_run_at"] = completed_run["finished_at"]
                    pipeline["last_run_status"] = completed_run["status"]
                    pipeline["updated_at"] = completed_run["finished_at"]
                    if trigger == "schedule":
                        pipeline["last_scheduled_run_at"] = completed_run["finished_at"]
                    break
            self._save_state()
            return completed_run

    def _finish_run_failure(self, run_id, pipeline_id, error_message, trigger):
        with self._lock:
            failed_run = None
            for run in self._state["runs"]:
                if run["id"] == run_id:
                    run.update(
                        {
                            "status": "failed",
                            "finished_at": utc_now(),
                            "error": error_message,
                        }
                    )
                    run["logs"].append(error_message)
                    failed_run = deepcopy(run)
                    break
            for pipeline in self._state["pipelines"]:
                if pipeline["id"] == pipeline_id:
                    pipeline["last_run_at"] = failed_run["finished_at"]
                    pipeline["last_run_status"] = failed_run["status"]
                    pipeline["updated_at"] = failed_run["finished_at"]
                    if trigger == "schedule":
                        pipeline["last_scheduled_run_at"] = failed_run["finished_at"]
                    break
            self._save_state()
            return failed_run

    def _validate_pipeline(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("Pipeline payload must be a JSON object.")

        name = payload.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Pipeline name is required.")

        source = self._validate_source(payload.get("source"))

        transformations = payload.get("transformations", [])
        if not isinstance(transformations, list):
            raise ValueError("Transformations must be a list.")
        for transformation in transformations:
            self._validate_transformation(transformation)

        destination = self._validate_destination(payload.get("destination"), name.strip())

        enabled = payload.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean.")

        schedule_interval_seconds = payload.get("schedule_interval_seconds")
        if schedule_interval_seconds is not None:
            if not isinstance(schedule_interval_seconds, int) or schedule_interval_seconds <= 0:
                raise ValueError("schedule_interval_seconds must be a positive integer when provided.")

        return {
            "name": name.strip(),
            "enabled": enabled,
            "schedule_interval_seconds": schedule_interval_seconds,
            "source": source,
            "transformations": transformations,
            "destination": destination,
        }

    def _validate_source(self, source):
        if not isinstance(source, dict):
            raise ValueError("Source is required.")
        source_type = source.get("type")
        config = source.get("config", {})

        if source_type == "inline_json":
            records = config.get("records")
            if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
                raise ValueError("inline_json requires a list of JSON objects.")
            return source

        if source_type == "csv_file":
            path = config.get("path")
            if not isinstance(path, str) or not path.strip():
                raise ValueError("csv_file requires a file path.")
            delimiter = config.get("delimiter", ",")
            if not isinstance(delimiter, str) or len(delimiter) != 1:
                raise ValueError("csv_file delimiter must be a single character.")
            has_header = config.get("has_header", True)
            if not isinstance(has_header, bool):
                raise ValueError("csv_file has_header must be a boolean.")
            field_names = config.get("field_names")
            if field_names is not None:
                if not isinstance(field_names, list) or any(not isinstance(field, str) for field in field_names):
                    raise ValueError("csv_file field_names must be a list of strings.")
            self._resolve_storage_path(path)
            return source

        if source_type == "http_json":
            url = config.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                raise ValueError("http_json requires an http or https URL.")
            records_key = config.get("records_key")
            if records_key is not None and not isinstance(records_key, str):
                raise ValueError("http_json records_key must be a string when provided.")
            timeout_seconds = config.get("timeout_seconds", 10)
            if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
                raise ValueError("http_json timeout_seconds must be a positive integer.")
            return source

        raise ValueError(f"Unsupported source type: {source_type}")

    def _validate_destination(self, destination, pipeline_name):
        if not isinstance(destination, dict):
            raise ValueError("Destination is required.")
        destination_type = destination.get("type")
        config = destination.get("config", {})

        if destination_type == "jsonl_file":
            path = config.get("path")
            if path is not None and not isinstance(path, str):
                raise ValueError("jsonl_file path must be a string when provided.")
            if path:
                self._resolve_storage_path(path)
            return destination

        if destination_type == "sqlite_file":
            path = config.get("path")
            if path is not None and not isinstance(path, str):
                raise ValueError("sqlite_file path must be a string when provided.")
            if path:
                self._resolve_storage_path(path)
            table = config.get("table") or slugify(pipeline_name)
            if not isinstance(table, str) or not table.strip():
                raise ValueError("sqlite_file table must be a non-empty string.")
            mode = config.get("mode", "replace")
            if mode not in {"replace", "append"}:
                raise ValueError("sqlite_file mode must be replace or append.")
            destination.setdefault("config", {})["table"] = table
            destination["config"]["mode"] = mode
            return destination

        raise ValueError(f"Unsupported destination type: {destination_type}")

    def _validate_transformation(self, transformation):
        if not isinstance(transformation, dict):
            raise ValueError("Each transformation must be an object.")
        transformation_type = transformation.get("type")
        config = transformation.get("config", {})

        if transformation_type == "rename_fields":
            mapping = config.get("mapping")
            if not isinstance(mapping, dict) or any(
                not isinstance(key, str) or not isinstance(value, str) for key, value in mapping.items()
            ):
                raise ValueError("rename_fields requires a string-to-string mapping.")
            return

        if transformation_type == "select_fields":
            fields = config.get("fields")
            if not isinstance(fields, list) or any(not isinstance(field, str) for field in fields):
                raise ValueError("select_fields requires a list of field names.")
            return

        if transformation_type == "filter_equals":
            if not isinstance(config.get("field"), str):
                raise ValueError("filter_equals requires a field name.")
            return

        if transformation_type == "add_fields":
            values = config.get("values")
            if not isinstance(values, dict):
                raise ValueError("add_fields requires a values object.")
            return

        if transformation_type == "uppercase_fields":
            fields = config.get("fields")
            if not isinstance(fields, list) or any(not isinstance(field, str) for field in fields):
                raise ValueError("uppercase_fields requires a list of field names.")
            return

        raise ValueError(f"Unsupported transformation type: {transformation_type}")

    def _extract(self, source):
        source_type = source["type"]
        config = source.get("config", {})

        if source_type == "inline_json":
            return deepcopy(config["records"])

        if source_type == "csv_file":
            source_path = self._resolve_storage_path(config["path"])
            self._ensure_safe_existing_file(source_path)
            delimiter = config.get("delimiter", ",")
            encoding = config.get("encoding", "utf-8")
            has_header = config.get("has_header", True)
            field_names = config.get("field_names")
            with source_path.open("r", encoding=encoding, newline="") as handle:
                if has_header:
                    reader = csv.DictReader(handle, delimiter=delimiter)
                    return [dict(row) for row in reader]
                if not field_names:
                    raise PipelineExecutionError("csv_file without headers requires field_names.")
                reader = csv.reader(handle, delimiter=delimiter)
                return [dict(zip(field_names, row)) for row in reader]

        if source_type == "http_json":
            try:
                with request.urlopen(config["url"], timeout=config.get("timeout_seconds", 10)) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except (error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
                raise PipelineExecutionError(f"Failed to fetch HTTP JSON source: {exc}") from exc

            records_key = config.get("records_key")
            if records_key:
                payload = payload.get(records_key)
            elif isinstance(payload, dict) and "records" in payload:
                payload = payload["records"]

            if not isinstance(payload, list) or any(not isinstance(record, dict) for record in payload):
                raise PipelineExecutionError("HTTP JSON source must resolve to a list of JSON objects.")
            return payload

        raise PipelineExecutionError(f"Unsupported source type at runtime: {source_type}")

    def _transform(self, records, transformations):
        transformed = [dict(record) for record in records]
        for transformation in transformations:
            transformation_type = transformation["type"]
            config = transformation.get("config", {})
            if transformation_type == "rename_fields":
                mapping = config["mapping"]
                transformed = [
                    {mapping.get(key, key): value for key, value in record.items()}
                    for record in transformed
                ]
            elif transformation_type == "select_fields":
                requested = set(config["fields"])
                transformed = [
                    {key: value for key, value in record.items() if key in requested}
                    for record in transformed
                ]
            elif transformation_type == "filter_equals":
                field = config["field"]
                expected = config.get("value")
                transformed = [record for record in transformed if record.get(field) == expected]
            elif transformation_type == "add_fields":
                extras = config["values"]
                transformed = [{**record, **extras} for record in transformed]
            elif transformation_type == "uppercase_fields":
                fields = set(config["fields"])
                next_records = []
                for record in transformed:
                    updated = dict(record)
                    for field in fields:
                        value = updated.get(field)
                        if isinstance(value, str):
                            updated[field] = value.upper()
                    next_records.append(updated)
                transformed = next_records
        return transformed

    def _load(self, records, pipeline):
        destination = pipeline["destination"]
        destination_type = destination["type"]
        config = destination.get("config", {})

        if destination_type == "jsonl_file":
            default_name = f"exports/{pipeline['id']}.jsonl"
            output_path = self._resolve_storage_path(config.get("path"), default_name)
            self._ensure_safe_creatable_file(output_path)
            try:
                with self._open_output_handle(output_path) as handle:
                    for record in records:
                        handle.write(json.dumps(record) + "\n")
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise PipelineExecutionError("Destination path must not use symlinks.") from exc
                raise PipelineExecutionError(f"Failed writing JSONL output: {exc}") from exc
            return {
                "type": "jsonl_file",
                "path": str(output_path),
                "row_count": len(records),
            }

        if destination_type == "sqlite_file":
            default_name = f"warehouse/{pipeline['id']}.db"
            output_path = self._resolve_storage_path(config.get("path"), default_name)
            self._ensure_safe_creatable_file(output_path)
            table_name = config.get("table") or slugify(pipeline["name"])
            mode = config.get("mode", "replace")
            try:
                row_count = self._load_sqlite(output_path, table_name, records, mode)
            except sqlite3.Error as exc:
                raise PipelineExecutionError(f"Failed loading SQLite destination: {exc}") from exc
            return {
                "type": "sqlite_file",
                "path": str(output_path),
                "table": table_name,
                "mode": mode,
                "row_count": row_count,
            }

        raise PipelineExecutionError(f"Unsupported destination type at runtime: {destination_type}")

    def _load_sqlite(self, output_path, table_name, records, mode):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_safe_creatable_file(output_path)
        connection = sqlite3.connect(output_path)
        try:
            cursor = connection.cursor()
            quoted_table = quote_identifier(table_name)
            if mode == "replace":
                cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")

            columns = sorted({key for record in records for key in record.keys()})
            if not columns:
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} (_raw_json TEXT)")
                connection.commit()
                return 0

            declared_types = []
            for column in columns:
                values = [record.get(column) for record in records if record.get(column) is not None]
                sqlite_type = self._infer_sqlite_type(values)
                declared_types.append(f"{quote_identifier(column)} {sqlite_type}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({', '.join(declared_types)})")

            if records:
                placeholders = ", ".join("?" for _ in columns)
                insert_columns = ", ".join(quote_identifier(column) for column in columns)
                rows = [tuple(self._sqlite_value(record.get(column)) for column in columns) for record in records]
                cursor.executemany(
                    f"INSERT INTO {quoted_table} ({insert_columns}) VALUES ({placeholders})",
                    rows,
                )
            connection.commit()
            return len(records)
        finally:
            connection.close()

    def _infer_sqlite_type(self, values):
        if not values:
            return "TEXT"
        if all(isinstance(value, bool) for value in values):
            return "INTEGER"
        if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
            return "INTEGER"
        if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
            return "REAL"
        return "TEXT"

    def _sqlite_value(self, value):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    def _resolve_storage_path(self, configured_path=None, default_name=None):
        base_dir = self.data_dir
        if configured_path:
            candidate = Path(configured_path)
            if not candidate.is_absolute():
                candidate = base_dir / candidate
        else:
            candidate = base_dir / default_name
        resolved = candidate.resolve()
        try:
            resolved.relative_to(base_dir)
        except ValueError as exc:
            raise ValueError("Path must stay within the platform data directory.") from exc
        return resolved

    def _ensure_real_parent_directories(self, target_path):
        current_path = self.data_dir
        for part in target_path.relative_to(self.data_dir).parts[:-1]:
            current_path = current_path / part
            if not current_path.exists():
                continue
            if current_path.is_symlink():
                raise PipelineExecutionError("Path must not use symlinks.")
            if not current_path.is_dir():
                raise PipelineExecutionError("Path parent must be a directory.")

    def _ensure_safe_existing_file(self, target_path):
        self._ensure_real_parent_directories(target_path)
        if not target_path.exists():
            raise PipelineExecutionError("Source file not found.")
        if target_path.is_symlink():
            raise PipelineExecutionError("Source path must not use symlinks.")
        if not target_path.is_file():
            raise PipelineExecutionError("Source path must point to a file.")

    def _ensure_safe_creatable_file(self, target_path):
        self._ensure_real_parent_directories(target_path)
        if target_path.exists() and target_path.is_symlink():
            raise PipelineExecutionError("Destination path must not use symlinks.")

    def _open_output_handle(self, output_path):
        self._ensure_safe_creatable_file(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_safe_creatable_file(output_path)
        relative_parts = output_path.relative_to(self.data_dir).parts
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW

        file_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            file_flags |= os.O_NOFOLLOW

        current_fd = os.open(self.data_dir, directory_flags)
        try:
            for part in relative_parts[:-1]:
                try:
                    os.mkdir(part, mode=0o755, dir_fd=current_fd)
                except FileExistsError:
                    pass
                next_fd = os.open(part, directory_flags, dir_fd=current_fd)
                os.close(current_fd)
                current_fd = next_fd
            file_fd = os.open(relative_parts[-1], file_flags, 0o644, dir_fd=current_fd)
            return os.fdopen(file_fd, "w", encoding="utf-8")
        finally:
            os.close(current_fd)


class ETLHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, handler_class, platform):
        super().__init__(server_address, handler_class)
        self.platform = platform

    def server_close(self):
        self.platform.stop_scheduler()
        super().server_close()


def build_handler(platform):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            path_parts = [part for part in parsed.path.split("/") if part]

            if parsed.path == "/":
                self._html(200, INDEX_HTML)
                return
            if parsed.path == "/api/health":
                self._json(200, {"status": "ok", **platform.scheduler_status()})
                return
            if parsed.path == "/api/dashboard":
                self._json(200, platform.get_dashboard())
                return
            if parsed.path == "/api/connectors":
                self._json(200, CONNECTORS)
                return
            if parsed.path == "/api/pipelines":
                self._json(200, {"pipelines": platform.list_pipelines()})
                return
            if len(path_parts) == 3 and path_parts[:2] == ["api", "pipelines"]:
                pipeline = platform.get_pipeline(path_parts[2])
                if pipeline is None:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                self._json(200, pipeline)
                return
            if parsed.path == "/api/runs":
                self._json(200, {"runs": platform.list_runs()})
                return
            if len(path_parts) == 3 and path_parts[:2] == ["api", "runs"]:
                run = platform.get_run(path_parts[2])
                if run is None:
                    self._json(404, {"error": "Run not found."})
                    return
                self._json(200, run)
                return
            if parsed.path == "/api/demo/contacts":
                self._json(
                    200,
                    {
                        "records": [
                            {"id": 1, "name": "Ada", "email": "ada@example.com", "country": "uk"},
                            {"id": 2, "name": "Grace", "email": "grace@example.com", "country": "us"},
                            {"id": 3, "name": "Linus", "email": "linus@example.com", "country": "fi"},
                        ]
                    },
                )
                return
            self._json(404, {"error": "Not found."})

        def do_POST(self):
            parsed = urlparse(self.path)
            path_parts = [part for part in parsed.path.split("/") if part]

            if parsed.path == "/api/pipelines":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                try:
                    pipeline = platform.create_pipeline(payload)
                except ValueError as exc:
                    self._json(400, {"error": str(exc)})
                    return
                self._json(201, pipeline)
                return

            if parsed.path == "/api/files":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                try:
                    saved_path = platform.write_text_file(payload.get("path"), payload.get("content"))
                except ValueError as exc:
                    self._json(400, {"error": str(exc)})
                    return
                except PipelineExecutionError as exc:
                    self._json(400, {"error": str(exc)})
                    return
                self._json(201, {"path": str(saved_path)})
                return

            if len(path_parts) == 4 and path_parts[:2] == ["api", "pipelines"] and path_parts[3] == "run":
                try:
                    run = platform.run_pipeline(path_parts[2], trigger="manual")
                except KeyError:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                except PipelineBusyError as exc:
                    self._json(409, {"error": str(exc)})
                    return
                self._json(201, run)
                return

            if len(path_parts) == 4 and path_parts[:2] == ["api", "pipelines"] and path_parts[3] == "preview":
                payload = self._read_json(required=False)
                limit = 20
                if payload and isinstance(payload.get("limit"), int) and payload["limit"] > 0:
                    limit = payload["limit"]
                try:
                    preview = platform.preview_pipeline(path_parts[2], limit=limit)
                except KeyError:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                except PipelineExecutionError as exc:
                    self._json(400, {"error": str(exc)})
                    return
                self._json(200, preview)
                return

            self._json(404, {"error": "Not found."})

        def do_DELETE(self):
            parsed = urlparse(self.path)
            path_parts = [part for part in parsed.path.split("/") if part]
            if len(path_parts) == 3 and path_parts[:2] == ["api", "pipelines"]:
                removed = platform.delete_pipeline(path_parts[2])
                if removed is None:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                self._json(200, {"deleted": removed["id"]})
                return
            self._json(404, {"error": "Not found."})

        def log_message(self, format, *args):  # noqa: A003
            return

        def _read_json(self, required):
            length_header = self.headers.get("Content-Length")
            if not length_header:
                if required:
                    self._json(400, {"error": "Missing request body."})
                    return None
                return None
            try:
                length = int(length_header)
            except ValueError:
                self._json(400, {"error": "Content-Length must be an integer."})
                return None
            if length == 0:
                if required:
                    self._json(400, {"error": "Missing request body."})
                    return None
                return None
            try:
                raw = self.rfile.read(length)
                return json.loads(raw.decode("utf-8"))
            except (ValueError, json.JSONDecodeError):
                self._json(400, {"error": "Request body must be valid JSON."})
                return None

        def _json(self, status_code, payload):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _html(self, status_code, html):
            body = html.encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def create_server(host="127.0.0.1", port=8000, data_dir="data"):
    platform = ETLPlatform(Path(data_dir) / "platform_state.json")
    platform.start_scheduler()
    return ETLHTTPServer((host, port), build_handler(platform), platform)


INDEX_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>ETL Platform</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: Arial, sans-serif; margin: 2rem; background: #f5f7fb; color: #182033; }
      h1, h2, h3 { margin-bottom: 0.5rem; }
      .grid { display: grid; grid-template-columns: 1.2fr 1fr; gap: 1rem; }
      .card { background: #fff; border-radius: 14px; padding: 1rem; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
      textarea, input, select { width: 100%; box-sizing: border-box; margin-bottom: 0.75rem; padding: 0.65rem; border: 1px solid #c9d2e3; border-radius: 8px; }
      textarea { min-height: 9rem; font-family: monospace; }
      button { border: 0; border-radius: 8px; background: #3458e6; color: #fff; padding: 0.65rem 0.95rem; cursor: pointer; margin-right: 0.5rem; margin-bottom: 0.5rem; }
      button.secondary { background: #61708a; }
      button.danger { background: #b42318; }
      ul { padding-left: 1rem; }
      li { margin-bottom: 0.75rem; }
      pre, code { background: #eef2ff; border-radius: 6px; }
      pre { padding: 0.8rem; overflow: auto; }
      .muted { color: #5f6b7a; }
      .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.75rem; margin-bottom: 1rem; }
      .summary .card { padding: 0.8rem; }
      .pill { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; font-size: 0.85rem; background: #e8efff; }
      .stack > * { margin-bottom: 0.5rem; }
    </style>
  </head>
  <body>
    <h1>ETL Platform</h1>
    <p class="muted">A fuller end-to-end single-node data movement platform inspired by Airbyte. It supports HTTP JSON, CSV, JSONL, SQLite, schedules, previews, run logs, and browser-driven management.</p>

    <section class="summary" id="summary"></section>

    <div class="grid">
      <section class="card">
        <h2>Create pipeline</h2>
        <input id="name" value="demo-contacts-sync" />
        <label>Source type</label>
        <select id="sourceType">
          <option value="http_json">http_json</option>
          <option value="inline_json">inline_json</option>
          <option value="csv_file">csv_file</option>
        </select>
        <textarea id="sourceConfig"></textarea>

        <label>Transformations</label>
        <textarea id="transformations"></textarea>

        <label>Destination type</label>
        <select id="destinationType">
          <option value="sqlite_file">sqlite_file</option>
          <option value="jsonl_file">jsonl_file</option>
        </select>
        <textarea id="destinationConfig"></textarea>

        <label>Schedule interval seconds (optional)</label>
        <input id="scheduleInterval" placeholder="60" />

        <button id="create">Create pipeline</button>
        <button id="previewCreate" class="secondary">Preview payload</button>
        <pre id="message"></pre>
      </section>

      <section class="card stack">
        <h2>Upload a CSV source file</h2>
        <input id="filePath" value="uploads/contacts.csv" />
        <textarea id="fileContent">id,name,email\n1,Ada,ada@example.com\n2,Grace,grace@example.com</textarea>
        <button id="saveFile">Save file</button>
        <h3>Built-in demo source</h3>
        <p class="muted">Use <code>/api/demo/contacts</code> from the default form to create a complete HTTP JSON to SQLite flow immediately.</p>
        <h3>Connectors</h3>
        <ul id="connectors"></ul>
      </section>
    </div>

    <section class="card" style="margin-top: 1rem;">
      <h2>Pipelines</h2>
      <ul id="pipelines"></ul>
    </section>

    <section class="card" style="margin-top: 1rem;">
      <h2>Runs</h2>
      <ul id="runs"></ul>
    </section>

    <script>
      const defaults = {
        inline_json: JSON.stringify({ records: [
          { id: 1, name: 'Ada', email: 'ada@example.com', country: 'uk' },
          { id: 2, name: 'Grace', email: 'grace@example.com', country: 'us' }
        ] }, null, 2),
        csv_file: JSON.stringify({ path: 'uploads/contacts.csv', delimiter: ',', has_header: true }, null, 2),
        http_json: JSON.stringify({ url: `${window.location.origin}/api/demo/contacts`, records_key: 'records', timeout_seconds: 10 }, null, 2),
        jsonl_file: JSON.stringify({ path: 'exports/demo-contacts.jsonl' }, null, 2),
        sqlite_file: JSON.stringify({ path: 'warehouse/demo-contacts.db', table: 'contacts', mode: 'replace' }, null, 2),
        transformations: JSON.stringify([
          { type: 'rename_fields', config: { mapping: { email: 'email_address' } } },
          { type: 'uppercase_fields', config: { fields: ['country'] } },
          { type: 'add_fields', config: { values: { synced_by: 'etl-platform' } } }
        ], null, 2)
      };

      const sourceType = document.getElementById('sourceType');
      const destinationType = document.getElementById('destinationType');
      const sourceConfig = document.getElementById('sourceConfig');
      const destinationConfig = document.getElementById('destinationConfig');
      const transformations = document.getElementById('transformations');
      const message = document.getElementById('message');

      function setDefaults() {
        sourceConfig.value = defaults[sourceType.value];
        destinationConfig.value = defaults[destinationType.value];
        transformations.value = defaults.transformations;
      }

      sourceType.addEventListener('change', () => {
        sourceConfig.value = defaults[sourceType.value];
      });
      destinationType.addEventListener('change', () => {
        destinationConfig.value = defaults[destinationType.value];
      });

      async function fetchJson(url, options) {
        const response = await fetch(url, options);
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || 'Request failed.');
        }
        return payload;
      }

      function renderSummary(summary) {
        const root = document.getElementById('summary');
        root.textContent = '';
        const cards = [
          ['Pipelines', summary.pipeline_count],
          ['Enabled', summary.enabled_pipeline_count],
          ['Runs', summary.run_count],
          ['Scheduler', summary.scheduler.running ? 'running' : 'stopped']
        ];
        cards.forEach(([label, value]) => {
          const card = document.createElement('div');
          card.className = 'card';
          const title = document.createElement('div');
          title.className = 'muted';
          title.textContent = label;
          const strong = document.createElement('strong');
          strong.textContent = String(value);
          card.appendChild(title);
          card.appendChild(strong);
          root.appendChild(card);
        });
      }

      function renderConnectors(connectors) {
        const root = document.getElementById('connectors');
        root.textContent = '';
        ['sources', 'transformations', 'destinations'].forEach((group) => {
          connectors[group].forEach((connector) => {
            const item = document.createElement('li');
            const label = document.createElement('code');
            label.textContent = connector.type;
            item.appendChild(label);
            item.appendChild(document.createTextNode(` — ${connector.description}`));
            root.appendChild(item);
          });
        });
      }

      function renderPipelines(pipelines) {
        const root = document.getElementById('pipelines');
        root.textContent = '';
        pipelines.forEach((pipeline) => {
          const item = document.createElement('li');
          const title = document.createElement('strong');
          title.textContent = pipeline.name;
          item.appendChild(title);
          item.appendChild(document.createTextNode(` `));

          const meta = document.createElement('span');
          meta.className = 'pill';
          meta.textContent = `${pipeline.source.type} → ${pipeline.destination.type}`;
          item.appendChild(meta);

          item.appendChild(document.createElement('br'));
          item.appendChild(document.createTextNode(`enabled=${pipeline.enabled} schedule=${pipeline.schedule_interval_seconds || 'manual'} last=${pipeline.last_run_status || 'never'}`));
          item.appendChild(document.createElement('br'));

          const runButton = document.createElement('button');
          runButton.textContent = 'Run now';
          runButton.addEventListener('click', async () => {
            try {
              const run = await fetchJson(`/api/pipelines/${pipeline.id}/run`, { method: 'POST' });
              message.textContent = JSON.stringify(run, null, 2);
              refresh();
            } catch (err) {
              message.textContent = err.message;
            }
          });

          const previewButton = document.createElement('button');
          previewButton.className = 'secondary';
          previewButton.textContent = 'Preview';
          previewButton.addEventListener('click', async () => {
            try {
              const preview = await fetchJson(`/api/pipelines/${pipeline.id}/preview`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ limit: 5 })
              });
              message.textContent = JSON.stringify(preview, null, 2);
            } catch (err) {
              message.textContent = err.message;
            }
          });

          const deleteButton = document.createElement('button');
          deleteButton.className = 'danger';
          deleteButton.textContent = 'Delete';
          deleteButton.addEventListener('click', async () => {
            try {
              await fetchJson(`/api/pipelines/${pipeline.id}`, { method: 'DELETE' });
              refresh();
            } catch (err) {
              message.textContent = err.message;
            }
          });

          item.appendChild(document.createElement('br'));
          item.appendChild(runButton);
          item.appendChild(previewButton);
          item.appendChild(deleteButton);
          root.appendChild(item);
        });
      }

      function renderRuns(runs) {
        const root = document.getElementById('runs');
        root.textContent = '';
        runs.forEach((run) => {
          const item = document.createElement('li');
          const title = document.createElement('strong');
          title.textContent = `${run.pipeline_name} — ${run.status}`;
          item.appendChild(title);
          item.appendChild(document.createTextNode(` (${run.trigger})`));
          item.appendChild(document.createElement('br'));
          item.appendChild(document.createTextNode(`records=${run.record_count} source=${run.source_type} destination=${run.destination_type}`));
          if (run.output) {
            const output = document.createElement('pre');
            output.textContent = JSON.stringify(run.output, null, 2);
            item.appendChild(output);
          }
          if (run.error) {
            const error = document.createElement('pre');
            error.textContent = run.error;
            item.appendChild(error);
          }
          if (run.logs && run.logs.length) {
            const logs = document.createElement('pre');
            logs.textContent = run.logs.join('\n');
            item.appendChild(logs);
          }
          root.appendChild(item);
        });
      }

      async function refresh() {
        const [dashboard, connectors, pipelines, runs] = await Promise.all([
          fetchJson('/api/dashboard'),
          fetchJson('/api/connectors'),
          fetchJson('/api/pipelines'),
          fetchJson('/api/runs')
        ]);
        renderSummary(dashboard);
        renderConnectors(connectors);
        renderPipelines(pipelines.pipelines);
        renderRuns(runs.runs);
      }

      document.getElementById('create').addEventListener('click', async () => {
        try {
          const scheduleRaw = document.getElementById('scheduleInterval').value.trim();
          const payload = {
            name: document.getElementById('name').value,
            enabled: true,
            source: { type: sourceType.value, config: JSON.parse(sourceConfig.value) },
            transformations: JSON.parse(transformations.value),
            destination: { type: destinationType.value, config: JSON.parse(destinationConfig.value) }
          };
          if (scheduleRaw) {
            payload.schedule_interval_seconds = Number(scheduleRaw);
          }
          const result = await fetchJson('/api/pipelines', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          message.textContent = JSON.stringify(result, null, 2);
          refresh();
        } catch (err) {
          message.textContent = err.message;
        }
      });

      document.getElementById('previewCreate').addEventListener('click', async () => {
        try {
          const payload = {
            name: document.getElementById('name').value,
            source: { type: sourceType.value, config: JSON.parse(sourceConfig.value) },
            transformations: JSON.parse(transformations.value),
            destination: { type: destinationType.value, config: JSON.parse(destinationConfig.value) }
          };
          message.textContent = JSON.stringify(payload, null, 2);
        } catch (err) {
          message.textContent = err.message;
        }
      });

      document.getElementById('saveFile').addEventListener('click', async () => {
        try {
          const result = await fetchJson('/api/files', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              path: document.getElementById('filePath').value,
              content: document.getElementById('fileContent').value
            })
          });
          message.textContent = JSON.stringify(result, null, 2);
        } catch (err) {
          message.textContent = err.message;
        }
      });

      setDefaults();
      refresh();
      setInterval(refresh, 3000);
    </script>
  </body>
</html>
"""


if __name__ == "__main__":
    server = create_server()
    print("Serving ETL platform at http://127.0.0.1:8000")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
