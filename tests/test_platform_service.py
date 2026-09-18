import sqlite3
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app import create_platform
from src.platform_service import PipelineExecutionError


class PlatformServiceTests(unittest.TestCase):
    def test_csv_to_sqlite_job_processed_by_worker_logic(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            csv_path = platform.write_text_file("uploads/orders.csv", "id,customer,total\n1,Ada,42\n2,Grace,91\n")
            pipeline = platform.create_pipeline(
                {
                    "name": "orders-sync",
                    "source": {"type": "csv_file", "config": {"path": str(csv_path), "has_header": True}},
                    "transformations": [{"type": "add_fields", "config": {"values": {"loaded_by": "worker"}}}],
                    "destination": {"type": "sqlite_file", "config": {"path": "warehouse/orders.db", "table": "orders"}},
                }
            )
            job = platform.enqueue_pipeline_run(pipeline["id"])
            self.assertEqual(job["status"], "queued")

            completed = platform.process_next_job()
            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(completed["record_count"], 2)

            with sqlite3.connect(Path(temp_dir) / "warehouse" / "orders.db") as connection:
                rows = connection.execute("SELECT id, customer, total, loaded_by FROM orders ORDER BY id").fetchall()
            self.assertEqual(rows, [("1", "Ada", "42", "worker"), ("2", "Grace", "91", "worker")])

    def test_http_discover_and_preview(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            records = [{"id": 1, "country": "uk"}, {"id": 2, "country": "us"}]
            pipeline = platform.create_pipeline(
                {
                    "name": "inline-preview",
                    "source": {"type": "inline_json", "config": {"records": records}},
                    "transformations": [{"type": "uppercase_fields", "config": {"fields": ["country"]}}],
                    "destination": {"type": "jsonl_file", "config": {"path": "exports/preview.jsonl"}},
                }
            )
            preview = platform.preview_pipeline(pipeline["id"], limit=5)
            self.assertEqual(preview["record_count"], 2)
            self.assertEqual(preview["preview"][0]["country"], "UK")
            discovered = platform.discover_source("inline_json", {"records": records})
            self.assertEqual(discovered["streams"][0]["fields"], ["country", "id"])

    def test_scheduled_pipeline_enqueues_jobs(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            base_time = time.time()
            pipeline = platform.create_pipeline(
                {
                    "name": "scheduled-sync",
                    "enabled": True,
                    "schedule_interval_seconds": 60,
                    "source": {"type": "inline_json", "config": {"records": [{"id": 1}] }},
                    "transformations": [],
                    "destination": {"type": "jsonl_file", "config": {"path": "exports/scheduled.jsonl"}},
                }
            )
            first = platform.run_due_schedules(now_epoch=base_time)
            self.assertEqual(len(first), 1)
            self.assertEqual(first[0]["status"], "queued")
            platform.process_next_job()
            second = platform.run_due_schedules(now_epoch=base_time + 10)
            self.assertEqual(second, [])
            third = platform.run_due_schedules(now_epoch=base_time + 100)
            self.assertEqual(len(third), 1)
            self.assertEqual(third[0]["pipeline_id"], pipeline["id"])

    def test_failed_job_captures_error(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            pipeline = platform.create_pipeline(
                {
                    "name": "broken-source",
                    "source": {"type": "csv_file", "config": {"path": "uploads/missing.csv", "has_header": True}},
                    "transformations": [],
                    "destination": {"type": "jsonl_file", "config": {"path": "exports/broken.jsonl"}},
                }
            )
            platform.enqueue_pipeline_run(pipeline["id"])
            completed = platform.process_next_job()
            self.assertEqual(completed["status"], "failed")
            self.assertIn("Source execution failed", completed["error"])
            self.assertTrue(any("Worker started job" in line for line in completed["logs"]))

    def test_safe_path_restriction_is_enforced(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            with self.assertRaises(ValueError):
                platform.create_pipeline(
                    {
                        "name": "unsafe",
                        "source": {"type": "inline_json", "config": {"records": [{"id": 1}] }},
                        "transformations": [],
                        "destination": {"type": "jsonl_file", "config": {"path": "../escape.jsonl"}},
                    }
                )
            with self.assertRaises(PipelineExecutionError):
                platform.ensure_safe_existing_file(Path(temp_dir) / "missing.csv")


if __name__ == "__main__":
    unittest.main()
