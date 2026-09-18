import http.client
import json
import sqlite3
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app import INDEX_HTML, create_platform
from src.api_server import create_server
from src.scheduler import SchedulerRunner
from src.worker import WorkerRunner


class APIServerTests(unittest.TestCase):
    def test_connector_lifecycle_endpoints(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            csv_path = platform.write_text_file("uploads/customers.csv", "id,name\n1,Ada\n2,Grace\n")
            server, thread, port, worker, scheduler = self._start_server(platform, embedded=False)
            try:
                status, spec = self._request(port, "GET", "/api/connectors/source/csv_file/spec")
                self.assertEqual(status, 200)
                self.assertEqual(spec["type"], "csv_file")
                status, check = self._request(port, "POST", "/api/connectors/source/csv_file/check", {"path": str(csv_path), "has_header": True})
                self.assertEqual(status, 200)
                self.assertTrue(check["ok"])
                status, discover = self._request(port, "POST", "/api/connectors/source/csv_file/discover", {"path": str(csv_path), "has_header": True})
                self.assertEqual(status, 200)
                self.assertEqual(discover["streams"][0]["fields"], ["id", "name"])
            finally:
                self._stop_server(server, thread, worker, scheduler)

    def test_async_job_flow_with_worker(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            worker = WorkerRunner(platform, poll_interval=0.1)
            scheduler = SchedulerRunner(platform, poll_interval=0.1)
            server, thread, port, _, _ = self._start_server(platform, embedded=False, worker=worker, scheduler=scheduler)
            worker.start_in_thread()
            try:
                create_status, pipeline = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "demo-http-sync",
                        "source": {"type": "http_json", "config": {"url": f"http://127.0.0.1:{port}/api/demo/contacts", "records_key": "records", "timeout_seconds": 10}},
                        "transformations": [{"type": "uppercase_fields", "config": {"fields": ["country"]}}],
                        "destination": {"type": "sqlite_file", "config": {"path": "warehouse/contacts.db", "table": "contacts"}},
                    },
                )
                self.assertEqual(create_status, 201)
                job_status, job = self._request(port, "POST", f"/api/pipelines/{pipeline['id']}/run")
                self.assertEqual(job_status, 202)
                self.assertEqual(job["status"], "queued")
                finished = self._wait_for_job(port, job["id"])
                self.assertEqual(finished["status"], "succeeded")
                with sqlite3.connect(Path(temp_dir) / "warehouse" / "contacts.db") as connection:
                    rows = connection.execute("SELECT name, country FROM contacts ORDER BY id").fetchall()
                self.assertEqual(rows[0], ("Ada", "UK"))
            finally:
                self._stop_server(server, thread, worker, scheduler)

    def test_scheduler_enqueues_jobs(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            worker = WorkerRunner(platform, poll_interval=0.1)
            scheduler = SchedulerRunner(platform, poll_interval=0.1)
            server, thread, port, _, _ = self._start_server(platform, embedded=False, worker=worker, scheduler=scheduler)
            try:
                create_status, pipeline = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "scheduled-http-sync",
                        "enabled": True,
                        "schedule_interval_seconds": 1,
                        "source": {"type": "inline_json", "config": {"records": [{"id": 1}] }},
                        "transformations": [],
                        "destination": {"type": "jsonl_file", "config": {"path": "exports/scheduled.jsonl"}},
                    },
                )
                self.assertEqual(create_status, 201)
                scheduled = platform.run_due_schedules(now_epoch=time.time())
                self.assertEqual(len(scheduled), 1)
                worker.process_once()
                jobs_status, jobs = self._request(port, "GET", "/api/jobs")
                self.assertEqual(jobs_status, 200)
                self.assertEqual(jobs["jobs"][0]["status"], "succeeded")
                self.assertEqual(jobs["jobs"][0]["pipeline_id"], pipeline["id"])
            finally:
                self._stop_server(server, thread, worker, scheduler)

    def test_file_upload_and_ui(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            server, thread, port, worker, scheduler = self._start_server(platform, embedded=False)
            try:
                status, upload = self._request(port, "POST", "/api/files", {"path": "uploads/sample.csv", "content": "id,name\n1,Ada\n"})
                self.assertEqual(status, 201)
                self.assertTrue(upload["path"].endswith("uploads/sample.csv"))
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                connection.request("GET", "/")
                response = connection.getresponse()
                html = response.read().decode("utf-8")
                connection.close()
                self.assertIn("queued jobs", html.lower())
                self.assertIn("textContent", INDEX_HTML)
            finally:
                self._stop_server(server, thread, worker, scheduler)

    def test_pipeline_state_endpoints(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)
            server, thread, port, worker, scheduler = self._start_server(platform, embedded=False)
            try:
                # Create pipeline
                status, pipe = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "state-test",
                        "sync_mode": "incremental",
                        "cursor_field": "updated_at",
                        "source": {"type": "inline_json", "config": {"records": [{"id": 1, "updated_at": "2026-01-01"}] }},
                        "transformations": [],
                        "destination": {"type": "jsonl_file", "config": {"path": "exports/state.jsonl"}},
                    },
                )
                self.assertEqual(status, 201)
                pipe_id = pipe["id"]

                # Check initial state is None
                status, state_data = self._request(port, "GET", f"/api/pipelines/{pipe_id}/state")
                self.assertEqual(status, 200)
                self.assertIsNone(state_data["state"])

                # Set state directly
                platform.set_pipeline_state(pipe_id, {"cursor_value": "2026-01-01"})
                status, state_data = self._request(port, "GET", f"/api/pipelines/{pipe_id}/state")
                self.assertEqual(status, 200)
                self.assertEqual(state_data["state"]["cursor_value"], "2026-01-01")

                # Reset state via API
                status, reset_res = self._request(port, "POST", f"/api/pipelines/{pipe_id}/state/reset", {})
                self.assertEqual(status, 200)
                self.assertTrue(reset_res["ok"])

                status, state_data = self._request(port, "GET", f"/api/pipelines/{pipe_id}/state")
                self.assertEqual(status, 200)
                self.assertIsNone(state_data["state"])
            finally:
                self._stop_server(server, thread, worker, scheduler)

    def _start_server(self, platform, embedded=False, worker=None, scheduler=None):
        worker = worker if worker is not None else WorkerRunner(platform, poll_interval=0.1)
        scheduler = scheduler if scheduler is not None else SchedulerRunner(platform, poll_interval=0.1)
        server = create_server("127.0.0.1", 0, platform, worker=worker if embedded else None, scheduler=scheduler if embedded else None)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, thread, server.server_address[1], worker, scheduler

    def _stop_server(self, server, thread, worker, scheduler):
        if worker:
            worker.stop()
        if scheduler:
            scheduler.stop()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    def _wait_for_job(self, port, job_id, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            status, job = self._request(port, "GET", f"/api/jobs/{job_id}")
            if status == 200 and job["status"] in {"succeeded", "failed"}:
                return job
            time.sleep(0.1)
        self.fail("job did not finish in time")

    def _request(self, port, method, path, payload=None):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        headers = {}
        body = None
        if payload is not None:
            body = json.dumps(payload)
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        data = json.loads(raw.decode("utf-8"))
        connection.close()
        return response.status, data


if __name__ == "__main__":
    unittest.main()
