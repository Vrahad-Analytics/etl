import http.client
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app import ETLPlatform, create_server


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
                run_status, run = self._request(
                    port, "POST", f"/api/pipelines/{pipeline['id']}/run", {}
                )
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
