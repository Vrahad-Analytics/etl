import http.client
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app import ETLPlatform, INDEX_HTML, create_server


class ETLPlatformTests(unittest.TestCase):
    def test_create_and_run_pipeline_writes_jsonl_output(self):
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
                        {
                            "type": "rename_fields",
                            "config": {"mapping": {"email": "email_address"}},
                        },
                        {
                            "type": "select_fields",
                            "config": {"fields": ["id", "name", "email_address"]},
                        },
                    ],
                    "destination": {
                        "type": "jsonl_file",
                        "config": {"path": str(output_path)},
                    },
                }
            )

            run = platform.run_pipeline(pipeline["id"])

            self.assertEqual(run["status"], "succeeded")
            self.assertEqual(run["record_count"], 2)
            self.assertEqual(Path(run["output_path"]), output_path)
            written_records = [
                json.loads(line)
                for line in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(
                written_records,
                [
                    {"id": 1, "name": "Ada", "email_address": "ada@example.com"},
                    {"id": 2, "name": "Grace", "email_address": "grace@example.com"},
                ],
            )

    def test_http_end_to_end_pipeline_flow(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]

            try:
                payload = {
                    "name": "orders-sync",
                    "source": {
                        "type": "inline_json",
                        "config": {"records": [{"id": "A-1", "amount": 42}]},
                    },
                    "transformations": [],
                    "destination": {
                        "type": "jsonl_file",
                        "config": {"path": str(Path(temp_dir) / "orders.jsonl")},
                    },
                }

                create_status, pipeline = self._request(
                    port, "POST", "/api/pipelines", payload
                )
                self.assertEqual(create_status, 201)
                run_status, run = self._request(port, "POST", f"/api/pipelines/{pipeline['id']}/run")
                self.assertEqual(run_status, 201)
                self.assertEqual(run["status"], "succeeded")

                get_status, runs = self._request(port, "GET", "/api/runs")
                self.assertEqual(get_status, 200)
                self.assertEqual(len(runs["runs"]), 1)
                self.assertEqual(runs["runs"][0]["id"], run["id"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_http_run_accepts_missing_request_body(self):
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
                        "name": "bodyless-run",
                        "source": {
                            "type": "inline_json",
                            "config": {"records": [{"id": 1}]},
                        },
                        "transformations": [],
                        "destination": {
                            "type": "jsonl_file",
                            "config": {"path": str(Path(temp_dir) / "bodyless.jsonl")},
                        },
                    },
                )
                self.assertEqual(create_status, 201)
                run_status, run = self._request(
                    port, "POST", f"/api/pipelines/{pipeline['id']}/run"
                )
                self.assertEqual(run_status, 201)
                self.assertEqual(run["status"], "succeeded")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_http_run_returns_validation_error_for_legacy_invalid_pipeline(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            server.platform._state["pipelines"].append(
                {
                    "id": "pipe_legacy",
                    "name": "legacy-invalid",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "source": {
                        "type": "inline_json",
                        "config": {"records": [{"id": 1}]},
                    },
                    "transformations": [],
                    "destination": {
                        "type": "jsonl_file",
                        "config": {"path": "../escape.jsonl"},
                    },
                }
            )
            server.platform._save_state()
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]

            try:
                status, body = self._request(port, "POST", "/api/pipelines/pipe_legacy/run")
                self.assertEqual(status, 400)
                self.assertIn("Destination path must stay within the platform data directory.", body["error"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_create_pipeline_rejects_destination_path_escape(self):
        with TemporaryDirectory() as temp_dir:
            platform = ETLPlatform(Path(temp_dir) / "platform_state.json")

            with self.assertRaisesRegex(
                ValueError, "Destination path must stay within the platform data directory."
            ):
                platform.create_pipeline(
                    {
                        "name": "unsafe-sync",
                        "source": {
                            "type": "inline_json",
                            "config": {"records": [{"id": 1}]},
                        },
                        "transformations": [],
                        "destination": {
                            "type": "jsonl_file",
                            "config": {"path": "../escape.jsonl"},
                        },
                    }
                )

    def test_http_rejects_destination_path_escape(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]

            try:
                status, body = self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "unsafe-sync",
                        "source": {
                            "type": "inline_json",
                            "config": {"records": [{"id": 1}]},
                        },
                        "transformations": [],
                        "destination": {
                            "type": "jsonl_file",
                            "config": {"path": "../escape.jsonl"},
                        },
                    },
                )
                self.assertEqual(status, 400)
                self.assertIn("Destination path must stay within the platform data directory.", body["error"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_invalid_run_route_shape_returns_not_found(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]

            try:
                status, body = self._request(port, "POST", "/api/pipelines/run")
                self.assertEqual(status, 404)
                self.assertEqual(body["error"], "Not found.")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_index_html_uses_text_based_rendering_for_dynamic_values(self):
        self.assertIn("document.createElement('li')", INDEX_HTML)
        self.assertIn("textContent = pipeline.name", INDEX_HTML)
        self.assertIn("textContent = run.output_path", INDEX_HTML)
        self.assertNotIn("pipelines').innerHTML", INDEX_HTML)
        self.assertNotIn("runs').innerHTML", INDEX_HTML)
        self.assertIn("addEventListener('click'", INDEX_HTML)

    def test_root_html_escapes_pipeline_and_run_values(self):
        with TemporaryDirectory() as temp_dir:
            server = create_server(host="127.0.0.1", port=0, data_dir=temp_dir)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]

            try:
                self._request(
                    port,
                    "POST",
                    "/api/pipelines",
                    {
                        "name": "<script>alert(1)</script>",
                        "source": {
                            "type": "inline_json",
                            "config": {"records": [{"id": 1}]},
                        },
                        "transformations": [],
                        "destination": {
                            "type": "jsonl_file",
                            "config": {"path": "exports/<b>safe</b>.jsonl"},
                        },
                    },
                )
                create_status, pipelines = self._request(port, "GET", "/api/pipelines")
                self.assertEqual(create_status, 200)
                pipeline_id = pipelines["pipelines"][0]["id"]
                self._request(port, "POST", f"/api/pipelines/{pipeline_id}/run")

                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                connection.request("GET", "/")
                response = connection.getresponse()
                html = response.read().decode("utf-8")
                connection.close()

                self.assertEqual(response.status, 200)
                self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
                self.assertIn("&lt;b&gt;safe&lt;/b&gt;.jsonl", html)
                self.assertNotIn("<script>alert(1)</script>", html)
                self.assertNotIn("<b>safe</b>.jsonl", html)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_http_rejects_symlink_destination_on_run(self):
        with TemporaryDirectory() as temp_dir:
            link_path = Path(temp_dir) / "exports" / "symlink.jsonl"
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
                        "name": "symlink-sync",
                        "source": {
                            "type": "inline_json",
                            "config": {"records": [{"id": 1}]},
                        },
                        "transformations": [],
                        "destination": {
                            "type": "jsonl_file",
                            "config": {"path": "exports/symlink.jsonl"},
                        },
                    },
                )
                self.assertEqual(create_status, 201)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(Path(temp_dir).parent / "outside.jsonl")
                run_status, run = self._request(
                    port, "POST", f"/api/pipelines/{pipeline['id']}/run"
                )
                self.assertEqual(run_status, 400)
                self.assertIn("Destination path", run["error"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def _request(self, port, method, path, payload=None):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
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
