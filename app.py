import json
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


CONNECTORS = {
    "sources": [
        {
            "type": "inline_json",
            "name": "Inline JSON",
            "description": "Provide records directly in the pipeline definition.",
        }
    ],
    "transformations": [
        {
            "type": "rename_fields",
            "name": "Rename fields",
            "description": "Renames keys based on a mapping.",
        },
        {
            "type": "select_fields",
            "name": "Select fields",
            "description": "Keeps only the requested fields.",
        },
    ],
    "destinations": [
        {
            "type": "jsonl_file",
            "name": "JSONL file",
            "description": "Writes newline-delimited JSON to a file.",
        }
    ],
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class ETLPlatform:
    def __init__(self, state_path):
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._state = self._load_state()

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

    def list_pipelines(self):
        with self._lock:
            return list(self._state["pipelines"])

    def get_pipeline(self, pipeline_id):
        with self._lock:
            for pipeline in self._state["pipelines"]:
                if pipeline["id"] == pipeline_id:
                    return pipeline
        return None

    def list_runs(self):
        with self._lock:
            return list(reversed(self._state["runs"]))

    def get_run(self, run_id):
        with self._lock:
            for run in self._state["runs"]:
                if run["id"] == run_id:
                    return run
        return None

    def create_pipeline(self, payload):
        pipeline = self._validate_pipeline(payload)
        pipeline["id"] = f"pipe_{uuid.uuid4().hex[:12]}"
        pipeline["created_at"] = utc_now()
        with self._lock:
            self._state["pipelines"].append(pipeline)
            self._save_state()
        return pipeline

    def run_pipeline(self, pipeline_id):
        with self._lock:
            pipeline = None
            for item in self._state["pipelines"]:
                if item["id"] == pipeline_id:
                    pipeline = item
                    break
            if pipeline is None:
                raise KeyError(f"Unknown pipeline: {pipeline_id}")

        run = {
            "id": f"run_{uuid.uuid4().hex[:12]}",
            "pipeline_id": pipeline_id,
            "status": "running",
            "started_at": utc_now(),
        }

        try:
            records = self._extract(pipeline["source"])
            records = self._transform(records, pipeline.get("transformations", []))
            output_path = self._load(records, pipeline["destination"], pipeline_id)
            run.update(
                {
                    "status": "succeeded",
                    "finished_at": utc_now(),
                    "record_count": len(records),
                    "output_path": str(output_path),
                    "preview": records[:5],
                }
            )
        except Exception as exc:  # pragma: no cover - defensive failure capture
            run.update(
                {
                    "status": "failed",
                    "finished_at": utc_now(),
                    "error": str(exc),
                }
            )

        with self._lock:
            self._state["runs"].append(run)
            self._save_state()
        return run

    def _validate_pipeline(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("Pipeline payload must be a JSON object.")

        name = payload.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Pipeline name is required.")

        source = payload.get("source")
        if not isinstance(source, dict) or source.get("type") != "inline_json":
            raise ValueError("Source type must be inline_json.")
        source_records = source.get("config", {}).get("records")
        if not isinstance(source_records, list) or any(not isinstance(item, dict) for item in source_records):
            raise ValueError("Source records must be a list of JSON objects.")

        transformations = payload.get("transformations", [])
        if not isinstance(transformations, list):
            raise ValueError("Transformations must be a list.")
        for transformation in transformations:
            self._validate_transformation(transformation)

        destination = payload.get("destination")
        if not isinstance(destination, dict) or destination.get("type") != "jsonl_file":
            raise ValueError("Destination type must be jsonl_file.")
        destination_path = destination.get("config", {}).get("path")
        if destination_path is not None and not isinstance(destination_path, str):
            raise ValueError("Destination path must be a string when provided.")

        return {
            "name": name.strip(),
            "source": source,
            "transformations": transformations,
            "destination": destination,
        }

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

        raise ValueError(f"Unsupported transformation type: {transformation_type}")

    def _extract(self, source):
        return list(source["config"]["records"])

    def _transform(self, records, transformations):
        transformed = list(records)
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
        return transformed

    def _load(self, records, destination, pipeline_id):
        configured_path = destination.get("config", {}).get("path")
        if configured_path:
            output_path = Path(configured_path)
        else:
            output_path = self.state_path.parent / f"{pipeline_id}.jsonl"

        if not output_path.is_absolute():
            output_path = Path.cwd() / output_path

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")
        return output_path


def build_handler(platform):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._html(200, INDEX_HTML)
                return
            if parsed.path == "/api/health":
                self._json(200, {"status": "ok"})
                return
            if parsed.path == "/api/connectors":
                self._json(200, CONNECTORS)
                return
            if parsed.path == "/api/pipelines":
                self._json(200, {"pipelines": platform.list_pipelines()})
                return
            if parsed.path.startswith("/api/pipelines/"):
                pipeline_id = parsed.path.rsplit("/", 1)[-1]
                pipeline = platform.get_pipeline(pipeline_id)
                if pipeline is None:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                self._json(200, pipeline)
                return
            if parsed.path == "/api/runs":
                self._json(200, {"runs": platform.list_runs()})
                return
            if parsed.path.startswith("/api/runs/"):
                run_id = parsed.path.rsplit("/", 1)[-1]
                run = platform.get_run(run_id)
                if run is None:
                    self._json(404, {"error": "Run not found."})
                    return
                self._json(200, run)
                return
            self._json(404, {"error": "Not found."})

        def do_POST(self):
            parsed = urlparse(self.path)
            payload = self._read_json()
            if payload is None:
                return

            if parsed.path == "/api/pipelines":
                try:
                    pipeline = platform.create_pipeline(payload)
                except ValueError as exc:
                    self._json(400, {"error": str(exc)})
                    return
                self._json(201, pipeline)
                return

            if parsed.path.startswith("/api/pipelines/") and parsed.path.endswith("/run"):
                pipeline_id = parsed.path[len("/api/pipelines/") : -len("/run")].strip("/")
                try:
                    run = platform.run_pipeline(pipeline_id)
                except KeyError:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                self._json(201, run)
                return

            self._json(404, {"error": "Not found."})

        def log_message(self, format, *args):  # noqa: A003
            return

        def _read_json(self):
            length = self.headers.get("Content-Length")
            if not length:
                self._json(400, {"error": "Missing request body."})
                return None
            try:
                raw = self.rfile.read(int(length))
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
    state_path = Path(data_dir) / "platform_state.json"
    platform = ETLPlatform(state_path)
    server = ThreadingHTTPServer((host, port), build_handler(platform))
    return server


INDEX_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>ETL Platform</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; background: #f7f9fc; color: #162033; }
      h1, h2 { margin-bottom: 0.5rem; }
      .grid { display: grid; grid-template-columns: 1.2fr 1fr; gap: 1.5rem; }
      .card { background: white; padding: 1rem; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); }
      textarea, input { width: 100%; box-sizing: border-box; margin-bottom: 0.75rem; padding: 0.6rem; }
      textarea { min-height: 9rem; font-family: monospace; }
      button { padding: 0.6rem 0.9rem; border: 0; border-radius: 8px; background: #3559e0; color: white; cursor: pointer; }
      ul { padding-left: 1rem; }
      li { margin-bottom: 0.75rem; }
      code { background: #eef2ff; padding: 0.15rem 0.35rem; border-radius: 4px; }
      .muted { color: #5f6b7a; }
    </style>
  </head>
  <body>
    <h1>ETL Platform</h1>
    <p class="muted">A minimal Airbyte-inspired ETL platform with connectors, transformations, runs, and output files.</p>
    <div class="grid">
      <section class="card">
        <h2>Create pipeline</h2>
        <input id="name" value="contacts-sync" />
        <textarea id="records">[
  {"id": 1, "name": "Ada", "email": "ada@example.com"},
  {"id": 2, "name": "Grace", "email": "grace@example.com"}
]</textarea>
        <textarea id="transformations">[
  {"type": "rename_fields", "config": {"mapping": {"email": "email_address"}}},
  {"type": "select_fields", "config": {"fields": ["id", "name", "email_address"]}}
]</textarea>
        <input id="destination" value="data/contacts-sync.jsonl" />
        <button id="create">Create pipeline</button>
        <pre id="message"></pre>
      </section>
      <section class="card">
        <h2>Available connectors</h2>
        <ul>
          <li><code>inline_json</code> source</li>
          <li><code>rename_fields</code> transform</li>
          <li><code>select_fields</code> transform</li>
          <li><code>jsonl_file</code> destination</li>
        </ul>
      </section>
    </div>
    <section class="card" style="margin-top: 1.5rem;">
      <h2>Pipelines</h2>
      <ul id="pipelines"></ul>
    </section>
    <section class="card" style="margin-top: 1.5rem;">
      <h2>Runs</h2>
      <ul id="runs"></ul>
    </section>
    <script>
      async function refresh() {
        const [pipelinesResponse, runsResponse] = await Promise.all([
          fetch('/api/pipelines'),
          fetch('/api/runs')
        ]);
        const pipelines = await pipelinesResponse.json();
        const runs = await runsResponse.json();

        document.getElementById('pipelines').innerHTML = pipelines.pipelines.map((pipeline) => `
          <li>
            <strong>${pipeline.name}</strong> <code>${pipeline.id}</code>
            <button data-id="${pipeline.id}">Run</button>
          </li>
        `).join('');

        document.querySelectorAll('#pipelines button').forEach((button) => {
          button.onclick = async () => {
            const response = await fetch(`/api/pipelines/${button.dataset.id}/run`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({})
            });
            const run = await response.json();
            document.getElementById('message').textContent = JSON.stringify(run, null, 2);
            refresh();
          };
        });

        document.getElementById('runs').innerHTML = runs.runs.map((run) => `
          <li>
            <strong>${run.status}</strong> ${run.pipeline_id} · ${run.record_count || 0} records
            ${run.output_path ? `<div><code>${run.output_path}</code></div>` : ''}
          </li>
        `).join('');
      }

      document.getElementById('create').onclick = async () => {
        try {
          const payload = {
            name: document.getElementById('name').value,
            source: {
              type: 'inline_json',
              config: { records: JSON.parse(document.getElementById('records').value) }
            },
            transformations: JSON.parse(document.getElementById('transformations').value),
            destination: {
              type: 'jsonl_file',
              config: { path: document.getElementById('destination').value }
            }
          };
          const response = await fetch('/api/pipelines', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          const result = await response.json();
          document.getElementById('message').textContent = JSON.stringify(result, null, 2);
          if (response.ok) {
            refresh();
          }
        } catch (error) {
          document.getElementById('message').textContent = error.message;
        }
      };

      refresh();
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
