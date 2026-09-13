import http.client
import json
import sqlite3
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app import ETLPlatform, INDEX_HTML, create_server


class ETLPlatformTests(unittest.TestCase):
    def test_inline_json_to_jsonl_pipeline(self):
        with TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "platform_state.json"
            output_path = Path(temp_dir) / "exports" / "contacts.jsonl"
            platform = ETLPlatform(state_path)
            pipeline = platform.create_pipeline(
                {
                    "name": "contacts-sync",
                    "source": {
                        "type": "inline_json",
                        "config": {
                            "records": [
                                {"id": 1, "name": "Ada", "email": "ada@example.com"},
                                {"id": 2, "name": "Grace", "email": "grace@example.com"},
                            ]
                        },
                    },
                    "transformations": [
                        {"type": "rename_fields", "config": {"mapping": {"email": "email_address"}}},
                        {"type": "select_fields", "config": {"fields": ["id", "name", "email_address"]}},
                    ],
                    "destination": {
                        "type": "jsonl_file",
                        "config": {"path": str(output_path)},
                    },
                }
            )

            run = platform.run_pipeline(pipeline["id"])

            self.assertEqual(run["status"], "succeeded")
            written_records = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                written_records,
                [
                    {"id": 1, "name": "Ada", "email_address": "ada@example.com"},
                    {"id": 2, "name": "Grace", "email_address": "grace@example.com"},
                ],
            )

    def test_csv_file_to_sqlite_pipeline(self):
        with TemporaryDirectory() as temp_dir:
            platform = ETLPlatform(Path(temp_dir) / "platform_state.json")
            csv_path = platform.write_text_file(
                "uploads/orders.csv",
                "id,customer,total\n1,Ada,42\n2,Grace,91\n",
            )
            database_path = Path(temp_dir) / "warehouse" / "orders.db"
            pipeline = platform.create_pipeline(
                {
                    "name": "orders-sync",
                    "source": {
                        "type": "csv_file",
                        "config": {"path": str(csv_path), "delimiter": ",", "has_header": True},
                    },
                    "transformations": [
                        {"type": "add_fields", "config": {"values": {"loaded_by": "test"}}},
                    ],
                    "destination": {
                        "type": "sqlite_file",
                        "config": {"path": str(database_path), "table": "orders", "mode": "replace"},
                    },
                }
            )

            run = platform.run_pipeline(pipeline["id"])

            self.assertEqual(run["status"], "succeeded")
            with sqlite3.connect(database_path) as connection:
                rows = connection.execute(
                    "SELECT id, customer, total, loaded_by FROM orders ORDER BY id"
                ).fetchall()
            self.assertEqual(rows, [("1", "Ada", "42", "test"), ("2", "Grace", "91", "test")])

    def test_http_pipeline_flow_via_api(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]
            try:
                payload = {
                    "name": "demo-http-sync",
                    "source": {
                        "type": "http_json",
                        "config": {
                            "url": f"http://127.0.0.1:{port}/api/demo/contacts",
                            "records_key": "records",
                            "timeout_seconds": 10,
                        },
                    },
                    "transformations": [
                        {"type": "uppercase_fields", "config": {"fields": ["country"]}},
                    ],
                    "destination": {
                        "type": "sqlite_file",
                        "config": {"path": "warehouse/contacts.db", "table": "contacts", "mode": "replace"},
                    },
                }
                create_status, pipeline = self._request(port, "POST", "/api/pipelines", payload)
                self.assertEqual(create_status, 201)
                run_status, run = self._request(port, "POST", f"/api/pipelines/{pipeline['id']}/run")
                self.assertEqual(run_status, 201)
                self.assertEqual(run["status"], "succeeded")
                self.assertEqual(run["record_count"], 3)

                database_path = Path(temp_dir) / "warehouse" / "contacts.db"
                with sqlite3.connect(database_path) as connection:
                    rows = connection.execute("SELECT name, country FROM contacts ORDER BY id").fetchall()
                self.assertEqual(rows[0], ("Ada", "UK"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_preview_endpoint_returns_transformed_records(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]
            try:
                create_status, pipeline = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "preview-sync",
                        "source": {
                            "type": "inline_json",
                            "config": {"records": [{"id": 1, "country": "uk"}, {"id": 2, "country": "us"}]},
                        },
                        "transformations": [
                            {"type": "uppercase_fields", "config": {"fields": ["country"]}},
                            {"type": "filter_equals", "config": {"field": "country", "value": "UK"}},
                        ],
                        "destination": {"type": "jsonl_file", "config": {"path": "exports/preview.jsonl"}},
                    },
                )
                self.assertEqual(create_status, 201)
                preview_status, preview = self._request(
                    port,
                    "POST",
                    f"/api/pipelines/{pipeline['id']}/preview",
                    {"limit": 10},
                )
                self.assertEqual(preview_status, 200)
                self.assertEqual(preview["record_count"], 1)
                self.assertEqual(preview["preview"][0]["country"], "UK")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_delete_pipeline_endpoint(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]
            try:
                create_status, pipeline = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "delete-me",
                        "source": {"type": "inline_json", "config": {"records": [{"id": 1}] }},
                        "transformations": [],
                        "destination": {"type": "jsonl_file", "config": {"path": "exports/delete.jsonl"}},
                    },
                )
                self.assertEqual(create_status, 201)
                delete_status, delete_result = self._request(port, "DELETE", f"/api/pipelines/{pipeline['id']}")
                self.assertEqual(delete_status, 200)
                self.assertEqual(delete_result["deleted"], pipeline["id"])
                get_status, body = self._request(port, "GET", f"/api/pipelines/{pipeline['id']}")
                self.assertEqual(get_status, 404)
                self.assertEqual(body["error"], "Pipeline not found.")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_scheduled_pipeline_runs_when_due(self):
        with TemporaryDirectory() as temp_dir:
            platform = ETLPlatform(Path(temp_dir) / "platform_state.json")
            base_time = time.time()
            pipeline = platform.create_pipeline(
                {
                    "name": "scheduled-sync",
                    "enabled": True,
                    "schedule_interval_seconds": 60,
                    "source": {"type": "inline_json", "config": {"records": [{"id": 1}, {"id": 2}] }},
                    "transformations": [],
                    "destination": {"type": "jsonl_file", "config": {"path": "exports/scheduled.jsonl"}},
                }
            )
            runs = platform.run_scheduled_pipelines_once(now_epoch=base_time)
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["status"], "succeeded")
            later_runs = platform.run_scheduled_pipelines_once(now_epoch=base_time + 10)
            self.assertEqual(later_runs, [])
            much_later_runs = platform.run_scheduled_pipelines_once(now_epoch=base_time + 100)
            self.assertEqual(len(much_later_runs), 1)
            self.assertEqual(much_later_runs[0]["pipeline_id"], pipeline["id"])

    def test_failed_run_captures_logs(self):
        with TemporaryDirectory() as temp_dir:
            platform = ETLPlatform(Path(temp_dir) / "platform_state.json")
            pipeline = platform.create_pipeline(
                {
                    "name": "broken-source",
                    "source": {"type": "csv_file", "config": {"path": "uploads/missing.csv", "has_header": True}},
                    "transformations": [],
                    "destination": {"type": "jsonl_file", "config": {"path": "exports/broken.jsonl"}},
                }
            )
            run = platform.run_pipeline(pipeline["id"])
            self.assertEqual(run["status"], "failed")
            self.assertIn("Source file not found.", run["error"])
            self.assertTrue(any("Starting manual run" in log for log in run["logs"]))

    def test_file_upload_endpoint_supports_csv_source(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]
            try:
                upload_status, upload = self._request(
                    port,
                    "POST",
                    "/api/files",
                    {"path": "uploads/customers.csv", "content": "id,name\n1,Ada\n2,Grace\n"},
                )
                self.assertEqual(upload_status, 201)
                self.assertTrue(upload["path"].endswith("uploads/customers.csv"))

                create_status, pipeline = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "uploaded-csv-sync",
                        "source": {"type": "csv_file", "config": {"path": "uploads/customers.csv", "has_header": True}},
                        "transformations": [],
                        "destination": {"type": "jsonl_file", "config": {"path": "exports/uploaded.jsonl"}},
                    },
                )
                self.assertEqual(create_status, 201)
                run_status, run = self._request(port, "POST", f"/api/pipelines/{pipeline['id']}/run")
                self.assertEqual(run_status, 201)
                self.assertEqual(run["status"], "succeeded")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_index_html_is_static_and_safe(self):
        self.assertIn("textContent", INDEX_HTML)
        self.assertIn("addEventListener", INDEX_HTML)
        self.assertNotIn("innerHTML =", INDEX_HTML)

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
