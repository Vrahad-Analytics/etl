import errno
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import connectors
from .state_db import StateDB


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


class PlatformService:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = StateDB(self.data_dir / "metadata.db")

    def slugify(self, value):
        letters = []
        for char in value.lower():
            letters.append(char if char.isalnum() else "_")
        result = "".join(letters).strip("_")
        while "__" in result:
            result = result.replace("__", "_")
        return result or "records"

    def quote_identifier(self, name):
        return '"' + name.replace('"', '""') + '"'

    def infer_sqlite_type(self, values):
        if not values:
            return "TEXT"
        if all(isinstance(value, bool) for value in values):
            return "INTEGER"
        if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
            return "INTEGER"
        if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
            return "REAL"
        return "TEXT"

    def sqlite_value(self, value):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (dict, list)):
            import json
            return json.dumps(value)
        return value

    def catalog(self):
        return connectors.catalog()

    def source_spec(self, name):
        return connectors.source_spec(name)

    def destination_spec(self, name):
        return connectors.destination_spec(name)

    def check_source(self, name, config):
        return connectors.source_check(name, config, self)

    def check_destination(self, name, config):
        return connectors.destination_check(name, config, self)

    def discover_source(self, name, config):
        try:
            return connectors.source_discover(name, config, self)
        except Exception as exc:
            raise PipelineExecutionError(str(exc)) from exc

    def list_pipelines(self):
        return self.db.list_pipelines()

    def get_pipeline(self, pipeline_id):
        return self.db.get_pipeline(pipeline_id)

    def delete_pipeline(self, pipeline_id):
        return self.db.delete_pipeline(pipeline_id)

    def get_pipeline_state(self, pipeline_id):
        return self.db.get_pipeline_state(pipeline_id)

    def set_pipeline_state(self, pipeline_id, state):
        return self.db.set_pipeline_state(pipeline_id, state, utc_now())

    def reset_pipeline_state(self, pipeline_id):
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        self.db.delete_pipeline_state(pipeline_id)
        return {"ok": True, "message": f"Reset state for pipeline {pipeline_id}"}

    def list_jobs(self):
        return self.db.list_jobs()

    def get_job(self, job_id):
        return self.db.get_job(job_id)

    def get_dashboard(self):
        pipelines = self.list_pipelines()
        jobs = self.list_jobs()
        return {
            "pipeline_count": len(pipelines),
            "enabled_pipeline_count": sum(1 for pipeline in pipelines if pipeline.get("enabled", True)),
            "job_count": len(jobs),
            "active_job_count": sum(1 for job in jobs if job["status"] in {"queued", "running"}),
            "latest_job": jobs[0] if jobs else None,
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
                "last_job_id": None,
                "last_scheduled_job_at": None,
            }
        )
        return self.db.create_pipeline(pipeline)

    def enqueue_pipeline_run(self, pipeline_id, trigger="manual"):
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        if self.db.has_active_job(pipeline_id):
            raise PipelineBusyError("Pipeline already has an active job.")
        now = utc_now()
        job = {
            "id": f"job_{uuid.uuid4().hex[:12]}",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline["name"],
            "trigger": trigger,
            "source_type": pipeline["source"]["type"],
            "destination_type": pipeline["destination"]["type"],
            "status": "queued",
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "record_count": 0,
            "preview": [],
            "output": None,
            "error": None,
            "logs": [f"Queued {trigger} job for {pipeline['name']}."] ,
        }
        self.db.create_job(job)
        self.db.mark_job_enqueued(pipeline_id, job["id"], now, trigger)
        return self.db.get_job(job["id"])

    def preview_pipeline(self, pipeline_id, limit=20):
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            raise KeyError(pipeline_id)
        records, _ = self._extract(pipeline["source"], pipeline=pipeline)
        records = self._transform(records, pipeline.get("transformations", []))
        return {
            "pipeline_id": pipeline_id,
            "record_count": len(records),
            "preview": records[:limit],
            "fields": sorted({key for record in records for key in record.keys()}),
        }

    def run_due_schedules(self, now_epoch=None):
        if now_epoch is None:
            now_epoch = time.time()
        jobs = []
        for pipeline in self.list_pipelines():
            interval = pipeline.get("schedule_interval_seconds")
            if not pipeline.get("enabled", True) or not interval:
                continue
            last_scheduled = pipeline.get("last_scheduled_job_at")
            if last_scheduled and now_epoch - iso_to_epoch(last_scheduled) < interval:
                continue
            try:
                jobs.append(self.enqueue_pipeline_run(pipeline["id"], trigger="schedule"))
            except PipelineBusyError:
                continue
        return jobs

    def process_next_job(self):
        job = self.db.claim_next_job(utc_now())
        if job is None:
            return None
        self.db.append_job_log(job["id"], f"Worker started job {job['id']}.")
        pipeline = self.get_pipeline(job["pipeline_id"])
        if pipeline is None:
            self.db.append_job_log(job["id"], "Pipeline no longer exists.")
            return self.db.finish_job(job["id"], "failed", utc_now(), error="Pipeline no longer exists.")
        try:
            self.db.append_job_log(job["id"], f"Reading source {pipeline['source']['type']} (sync mode: {pipeline.get('sync_mode', 'full_refresh')}).")
            state = self.get_pipeline_state(pipeline["id"]) if pipeline.get("sync_mode") == "incremental" else None
            if state:
                self.db.append_job_log(job["id"], f"Found previous stream state checkpoint: {state}")
            records, new_state = self._extract(pipeline["source"], pipeline=pipeline, state=state)
            self.db.append_job_log(job["id"], f"Read {len(records)} record(s).")
            records = self._transform(records, pipeline.get("transformations", []))
            self.db.append_job_log(job["id"], f"Transformed {len(records)} record(s).")
            output = self._load(records, pipeline)
            self.db.append_job_log(job["id"], f"Loaded {len(records)} record(s) into {pipeline['destination']['type']}.")
            if new_state is not None:
                self.set_pipeline_state(pipeline["id"], new_state)
                self.db.append_job_log(job["id"], f"Committed new state checkpoint: {new_state}")
            return self.db.finish_job(
                job["id"],
                "succeeded",
                utc_now(),
                record_count=len(records),
                preview=records[:5],
                output=output,
            )
        except Exception as exc:
            self.db.append_job_log(job["id"], str(exc))
            return self.db.finish_job(job["id"], "failed", utc_now(), error=str(exc))

    def write_text_file(self, relative_path, content):
        if not isinstance(relative_path, str) or not relative_path.strip():
            raise ValueError("File path is required.")
        if not isinstance(content, str):
            raise ValueError("File content must be a string.")
        output_path = self.resolve_storage_path(relative_path)
        self.ensure_safe_creatable_file(output_path)
        with self.open_output_handle(output_path) as handle:
            handle.write(content)
        return output_path

    def _validate_pipeline(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("Pipeline payload must be a JSON object.")
        name = payload.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Pipeline name is required.")

        enabled = payload.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean.")

        schedule_interval_seconds = payload.get("schedule_interval_seconds")
        if schedule_interval_seconds is not None and (
            not isinstance(schedule_interval_seconds, int) or schedule_interval_seconds <= 0
        ):
            raise ValueError("schedule_interval_seconds must be a positive integer when provided.")

        sync_mode = payload.get("sync_mode", "full_refresh")
        if sync_mode not in {"full_refresh", "incremental"}:
            raise ValueError("sync_mode must be full_refresh or incremental.")

        cursor_field = payload.get("cursor_field")
        if cursor_field is not None and (not isinstance(cursor_field, str) or not cursor_field.strip()):
            raise ValueError("cursor_field must be a non-empty string when provided.")

        primary_key = payload.get("primary_key")
        if primary_key is not None and (not isinstance(primary_key, str) or not primary_key.strip()):
            raise ValueError("primary_key must be a non-empty string when provided.")

        source = self._validate_source(payload.get("source"))
        transformations = payload.get("transformations", [])
        if not isinstance(transformations, list):
            raise ValueError("Transformations must be a list.")
        for transformation in transformations:
            self._validate_transformation(transformation)
        destination = self._validate_destination(payload.get("destination"), name.strip(), sync_mode=sync_mode)

        return {
            "name": name.strip(),
            "enabled": enabled,
            "schedule_interval_seconds": schedule_interval_seconds,
            "sync_mode": sync_mode,
            "cursor_field": cursor_field.strip() if cursor_field else None,
            "primary_key": primary_key.strip() if primary_key else None,
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
            if field_names is not None and (
                not isinstance(field_names, list) or any(not isinstance(field, str) for field in field_names)
            ):
                raise ValueError("csv_file field_names must be a list of strings.")
            self.resolve_storage_path(path)
            return source

        if source_type == "http_json":
            url = config.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                raise ValueError("http_json requires an http or https URL.")
            timeout_seconds = config.get("timeout_seconds", 10)
            if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
                raise ValueError("http_json timeout_seconds must be a positive integer.")
            records_key = config.get("records_key")
            if records_key is not None and not isinstance(records_key, str):
                raise ValueError("http_json records_key must be a string when provided.")
            return source

        raise ValueError(f"Unsupported source type: {source_type}")

    def _validate_destination(self, destination, pipeline_name, sync_mode="full_refresh"):
        if not isinstance(destination, dict):
            raise ValueError("Destination is required.")
        destination_type = destination.get("type")
        config = destination.get("config", {})

        if destination_type == "jsonl_file":
            path = config.get("path")
            if path is not None and not isinstance(path, str):
                raise ValueError("jsonl_file path must be a string when provided.")
            if path:
                self.resolve_storage_path(path)
            return destination

        if destination_type == "sqlite_file":
            path = config.get("path")
            if path is not None and not isinstance(path, str):
                raise ValueError("sqlite_file path must be a string when provided.")
            if path:
                self.resolve_storage_path(path)
            table = config.get("table") or self.slugify(pipeline_name)
            if not isinstance(table, str) or not table.strip():
                raise ValueError("sqlite_file table must be a non-empty string.")
            mode = config.get("mode")
            if not mode:
                mode = "append" if sync_mode == "incremental" else "replace"
            if mode not in {"replace", "append"}:
                raise ValueError("sqlite_file mode must be replace or append.")
            destination = {"type": destination_type, "config": dict(config)}
            destination["config"]["table"] = table
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
            if not isinstance(config.get("values"), dict):
                raise ValueError("add_fields requires a values object.")
            return
        if transformation_type == "uppercase_fields":
            fields = config.get("fields")
            if not isinstance(fields, list) or any(not isinstance(field, str) for field in fields):
                raise ValueError("uppercase_fields requires a list of field names.")
            return
        raise ValueError(f"Unsupported transformation type: {transformation_type}")

    def _extract(self, source, pipeline=None, state=None):
        try:
            cursor_field = pipeline.get("cursor_field") if pipeline else None
            return connectors.read_source(
                source["type"],
                source.get("config", {}),
                self,
                state=state,
                cursor_field=cursor_field,
            )
        except Exception as exc:
            raise PipelineExecutionError(f"Source execution failed: {exc}") from exc

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
                transformed = [record for record in transformed if record.get(config["field"]) == config.get("value")]
            elif transformation_type == "add_fields":
                transformed = [{**record, **config["values"]} for record in transformed]
            elif transformation_type == "uppercase_fields":
                fields = set(config["fields"])
                updated_records = []
                for record in transformed:
                    updated = dict(record)
                    for field in fields:
                        value = updated.get(field)
                        if isinstance(value, str):
                            updated[field] = value.upper()
                    updated_records.append(updated)
                transformed = updated_records
        return transformed

    def _load(self, records, pipeline):
        try:
            return connectors.write_destination(
                pipeline["destination"]["type"],
                pipeline["destination"].get("config", {}),
                records,
                self,
                pipeline,
            )
        except Exception as exc:
            raise PipelineExecutionError(f"Destination execution failed: {exc}") from exc

    def resolve_storage_path(self, configured_path=None, default_name=None):
        if configured_path:
            target = Path(configured_path)
            if not target.is_absolute():
                target = self.data_dir / target
        else:
            if not default_name:
                raise ValueError("Path is required.")
            target = self.data_dir / default_name
        resolved = target.resolve()
        try:
            resolved.relative_to(self.data_dir)
        except ValueError as exc:
            raise ValueError("Path must stay within the platform data directory.") from exc
        return resolved

    def ensure_real_parent_directories(self, target_path):
        target_path = Path(target_path).resolve()
        current_path = self.data_dir
        for part in target_path.relative_to(self.data_dir).parts[:-1]:
            current_path = current_path / part
            if not current_path.exists():
                continue
            if current_path.is_symlink():
                raise PipelineExecutionError("Path must not use symlinks.")
            if not current_path.is_dir():
                raise PipelineExecutionError("Path parent must be a directory.")

    def ensure_safe_existing_file(self, target_path):
        target_path = Path(target_path).resolve()
        self.ensure_real_parent_directories(target_path)
        if not target_path.exists():
            raise PipelineExecutionError("Source file not found.")
        if target_path.is_symlink():
            raise PipelineExecutionError("Source path must not use symlinks.")
        if not target_path.is_file():
            raise PipelineExecutionError("Source path must point to a file.")

    def ensure_safe_creatable_file(self, target_path):
        target_path = Path(target_path).resolve()
        self.ensure_real_parent_directories(target_path)
        if target_path.exists() and target_path.is_symlink():
            raise PipelineExecutionError("Destination path must not use symlinks.")

    def open_output_handle(self, output_path, mode="w"):
        output_path = Path(output_path).resolve()
        self.ensure_safe_creatable_file(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_safe_creatable_file(output_path)
        relative_parts = output_path.relative_to(self.data_dir).parts
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        if mode == "a":
            file_flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        else:
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
            return os.fdopen(file_fd, mode, encoding="utf-8")
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise PipelineExecutionError("Path must not use symlinks.") from exc
            raise
        finally:
            os.close(current_fd)
