import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


def _json_load(value, default):
    if not value:
        return default
    return json.loads(value)


class StateDB:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _raw_connect(self):
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _connect(self):
        connection = self._raw_connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS pipelines (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    schedule_interval_seconds INTEGER,
                    source_json TEXT NOT NULL,
                    transformations_json TEXT NOT NULL,
                    destination_json TEXT NOT NULL,
                    sync_mode TEXT NOT NULL DEFAULT 'full_refresh',
                    cursor_field TEXT,
                    primary_key TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_run_at TEXT,
                    last_run_status TEXT,
                    last_job_id TEXT,
                    last_scheduled_job_at TEXT
                );

                CREATE TABLE IF NOT EXISTS pipeline_states (
                    pipeline_id TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(pipeline_id) REFERENCES pipelines(id)
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    pipeline_name TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    destination_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    record_count INTEGER NOT NULL DEFAULT 0,
                    preview_json TEXT,
                    output_json TEXT,
                    error TEXT,
                    logs_json TEXT NOT NULL,
                    FOREIGN KEY(pipeline_id) REFERENCES pipelines(id)
                );
                """
            )
            # Automatic column migration for existing tables
            cursor = connection.cursor()
            cursor.execute("PRAGMA table_info(pipelines)")
            cols = {row["name"] for row in cursor.fetchall()}
            if "sync_mode" not in cols:
                cursor.execute("ALTER TABLE pipelines ADD COLUMN sync_mode TEXT NOT NULL DEFAULT 'full_refresh'")
            if "cursor_field" not in cols:
                cursor.execute("ALTER TABLE pipelines ADD COLUMN cursor_field TEXT")
            if "primary_key" not in cols:
                cursor.execute("ALTER TABLE pipelines ADD COLUMN primary_key TEXT")

    def _pipeline_from_row(self, row):
        if row is None:
            return None
        row_keys = row.keys() if hasattr(row, "keys") else []
        return {
            "id": row["id"],
            "name": row["name"],
            "enabled": bool(row["enabled"]),
            "schedule_interval_seconds": row["schedule_interval_seconds"],
            "source": json.loads(row["source_json"]),
            "transformations": json.loads(row["transformations_json"]),
            "destination": json.loads(row["destination_json"]),
            "sync_mode": row["sync_mode"] if "sync_mode" in row_keys else "full_refresh",
            "cursor_field": row["cursor_field"] if "cursor_field" in row_keys else None,
            "primary_key": row["primary_key"] if "primary_key" in row_keys else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_run_at": row["last_run_at"],
            "last_run_status": row["last_run_status"],
            "last_job_id": row["last_job_id"],
            "last_scheduled_job_at": row["last_scheduled_job_at"],
        }

    def _job_from_row(self, row):
        if row is None:
            return None
        return {
            "id": row["id"],
            "pipeline_id": row["pipeline_id"],
            "pipeline_name": row["pipeline_name"],
            "trigger": row["trigger"],
            "source_type": row["source_type"],
            "destination_type": row["destination_type"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "record_count": row["record_count"],
            "preview": _json_load(row["preview_json"], []),
            "output": _json_load(row["output_json"], None),
            "error": row["error"],
            "logs": _json_load(row["logs_json"], []),
        }

    def create_pipeline(self, pipeline):
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO pipelines (
                    id, name, enabled, schedule_interval_seconds, source_json,
                    transformations_json, destination_json, sync_mode, cursor_field,
                    primary_key, created_at, updated_at, last_run_at, last_run_status,
                    last_job_id, last_scheduled_job_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pipeline["id"],
                    pipeline["name"],
                    int(pipeline["enabled"]),
                    pipeline["schedule_interval_seconds"],
                    json.dumps(pipeline["source"]),
                    json.dumps(pipeline["transformations"]),
                    json.dumps(pipeline["destination"]),
                    pipeline.get("sync_mode", "full_refresh"),
                    pipeline.get("cursor_field"),
                    pipeline.get("primary_key"),
                    pipeline["created_at"],
                    pipeline["updated_at"],
                    pipeline["last_run_at"],
                    pipeline["last_run_status"],
                    pipeline["last_job_id"],
                    pipeline["last_scheduled_job_at"],
                ),
            )
        return pipeline

    def list_pipelines(self):
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM pipelines ORDER BY created_at DESC, id DESC").fetchall()
        return [self._pipeline_from_row(row) for row in rows]

    def get_pipeline(self, pipeline_id):
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)).fetchone()
        return self._pipeline_from_row(row)

    def delete_pipeline(self, pipeline_id):
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            return None
        with self._connect() as connection:
            connection.execute("DELETE FROM pipeline_states WHERE pipeline_id = ?", (pipeline_id,))
            connection.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        return pipeline

    def get_pipeline_state(self, pipeline_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_json FROM pipeline_states WHERE pipeline_id = ?",
                (pipeline_id,),
            ).fetchone()
        if row is None:
            return None
        return _json_load(row["state_json"], None)

    def set_pipeline_state(self, pipeline_id, state, updated_at):
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO pipeline_states (pipeline_id, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(pipeline_id) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (pipeline_id, json.dumps(state), updated_at),
            )
        return state

    def delete_pipeline_state(self, pipeline_id):
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM pipeline_states WHERE pipeline_id = ?",
                (pipeline_id,),
            )

    def has_active_job(self, pipeline_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM jobs WHERE pipeline_id = ? AND status IN ('queued', 'running') LIMIT 1",
                (pipeline_id,),
            ).fetchone()
        return row is not None

    def create_job(self, job):
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    id, pipeline_id, pipeline_name, trigger, source_type, destination_type,
                    status, created_at, started_at, finished_at, record_count, preview_json,
                    output_json, error, logs_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job["id"],
                    job["pipeline_id"],
                    job["pipeline_name"],
                    job["trigger"],
                    job["source_type"],
                    job["destination_type"],
                    job["status"],
                    job["created_at"],
                    job["started_at"],
                    job["finished_at"],
                    job["record_count"],
                    json.dumps(job["preview"]),
                    json.dumps(job["output"]),
                    job["error"],
                    json.dumps(job["logs"]),
                ),
            )
        return job

    def mark_job_enqueued(self, pipeline_id, job_id, at_time, trigger):
        with self._connect() as connection:
            if trigger == "schedule":
                connection.execute(
                    """
                    UPDATE pipelines
                    SET last_job_id = ?, updated_at = ?, last_scheduled_job_at = ?
                    WHERE id = ?
                    """,
                    (job_id, at_time, at_time, pipeline_id),
                )
            else:
                connection.execute(
                    "UPDATE pipelines SET last_job_id = ?, updated_at = ? WHERE id = ?",
                    (job_id, at_time, pipeline_id),
                )

    def claim_next_job(self, started_at):
        connection = self._raw_connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC, id ASC LIMIT 1"
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            updated = connection.execute(
                "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ? AND status = 'queued'",
                (started_at, row["id"]),
            )
            if updated.rowcount != 1:
                connection.rollback()
                return None
            refreshed = connection.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
            connection.commit()
            return self._job_from_row(refreshed)
        finally:
            connection.close()

    def append_job_log(self, job_id, message):
        with self._connect() as connection:
            row = connection.execute("SELECT logs_json FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                return None
            logs = _json_load(row["logs_json"], [])
            logs.append(message)
            connection.execute("UPDATE jobs SET logs_json = ? WHERE id = ?", (json.dumps(logs), job_id))
            refreshed = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job_from_row(refreshed)

    def finish_job(self, job_id, status, finished_at, record_count=0, preview=None, output=None, error=None):
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs
                SET status = ?, finished_at = ?, record_count = ?, preview_json = ?, output_json = ?, error = ?
                WHERE id = ?
                """,
                (
                    status,
                    finished_at,
                    record_count,
                    json.dumps(preview or []),
                    json.dumps(output),
                    error,
                    job_id,
                ),
            )
            job_row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if job_row is not None:
                connection.execute(
                    """
                    UPDATE pipelines
                    SET last_run_at = ?, last_run_status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (finished_at, status, finished_at, job_row["pipeline_id"]),
                )
        return self.get_job(job_id)

    def list_jobs(self):
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM jobs ORDER BY created_at DESC, id DESC").fetchall()
        return [self._job_from_row(row) for row in rows]

    def get_job(self, job_id):
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job_from_row(row)
