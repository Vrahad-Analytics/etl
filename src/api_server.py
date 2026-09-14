import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .platform_service import PipelineBusyError, PipelineExecutionError


INDEX_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>ETL Platform</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; background: #f5f7fb; color: #182033; }
      h1, h2, h3 { margin-bottom: 0.5rem; }
      .grid { display: grid; grid-template-columns: 1.3fr 1fr; gap: 1rem; }
      .card { background: #fff; border-radius: 14px; padding: 1rem; box-shadow: 0 2px 12px rgba(0,0,0,0.08); margin-bottom: 1rem; }
      textarea, input, select { width: 100%; box-sizing: border-box; margin-bottom: 0.75rem; padding: 0.65rem; border: 1px solid #c9d2e3; border-radius: 8px; }
      textarea { min-height: 9rem; font-family: monospace; }
      button { border: 0; border-radius: 8px; background: #3458e6; color: #fff; padding: 0.65rem 0.95rem; cursor: pointer; margin-right: 0.5rem; margin-bottom: 0.5rem; }
      button.secondary { background: #61708a; }
      button.danger { background: #b42318; }
      ul { padding-left: 1rem; }
      li { margin-bottom: 0.75rem; }
      pre, code { background: #eef2ff; border-radius: 6px; }
      pre { padding: 0.8rem; overflow: auto; white-space: pre-wrap; }
      .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.75rem; }
      .pill { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; font-size: 0.85rem; background: #e8efff; }
      .muted { color: #5f6b7a; }
    </style>
  </head>
  <body>
    <h1>ETL Platform</h1>
    <p class="muted">Airbyte-like local architecture with connector lifecycle endpoints, queued jobs, a worker, a scheduler, Docker support, and built-in demo data.</p>
    <section class="summary" id="summary"></section>
    <div class="grid">
      <section class="card">
        <h2>Create pipeline</h2>
        <input id="name" value="demo-http-sync" />
        <label>Source connector</label>
        <select id="sourceType">
          <option value="http_json">http_json</option>
          <option value="csv_file">csv_file</option>
          <option value="inline_json">inline_json</option>
        </select>
        <textarea id="sourceConfig"></textarea>
        <label>Transformations</label>
        <textarea id="transformations"></textarea>
        <label>Destination connector</label>
        <select id="destinationType">
          <option value="sqlite_file">sqlite_file</option>
          <option value="jsonl_file">jsonl_file</option>
        </select>
        <textarea id="destinationConfig"></textarea>
        <label>Schedule interval seconds (optional)</label>
        <input id="scheduleInterval" placeholder="60" />
        <button id="create">Create pipeline</button>
        <button id="previewPayload" class="secondary">Preview payload</button>
        <pre id="message"></pre>
      </section>
      <section>
        <section class="card">
          <h2>Upload source file</h2>
          <input id="filePath" value="uploads/contacts.csv" />
          <textarea id="fileContent">id,name,email,country\n1,Ada,ada@example.com,uk\n2,Grace,grace@example.com,us</textarea>
          <button id="saveFile">Save file</button>
        </section>
        <section class="card">
          <h2>Connectors</h2>
          <ul id="connectors"></ul>
        </section>
      </section>
    </div>
    <section class="card">
      <h2>Pipelines</h2>
      <ul id="pipelines"></ul>
    </section>
    <section class="card">
      <h2>Jobs</h2>
      <ul id="jobs"></ul>
    </section>
    <script>
      const defaults = {
        source: {
          http_json: JSON.stringify({ url: `${window.location.origin}/api/demo/contacts`, records_key: 'records', timeout_seconds: 10 }, null, 2),
          csv_file: JSON.stringify({ path: 'uploads/contacts.csv', delimiter: ',', has_header: true }, null, 2),
          inline_json: JSON.stringify({ records: [{ id: 1, name: 'Ada', email: 'ada@example.com' }] }, null, 2)
        },
        destination: {
          sqlite_file: JSON.stringify({ path: 'warehouse/contacts.db', table: 'contacts', mode: 'replace' }, null, 2),
          jsonl_file: JSON.stringify({ path: 'exports/contacts.jsonl' }, null, 2)
        },
        transformations: JSON.stringify([
          { type: 'rename_fields', config: { mapping: { email: 'email_address' } } },
          { type: 'uppercase_fields', config: { fields: ['country'] } },
          { type: 'add_fields', config: { values: { synced_by: 'ui' } } }
        ], null, 2)
      };

      const sourceType = document.getElementById('sourceType');
      const destinationType = document.getElementById('destinationType');
      const sourceConfig = document.getElementById('sourceConfig');
      const destinationConfig = document.getElementById('destinationConfig');
      const transformations = document.getElementById('transformations');
      const message = document.getElementById('message');

      function setDefaults() {
        sourceConfig.value = defaults.source[sourceType.value];
        destinationConfig.value = defaults.destination[destinationType.value];
        transformations.value = defaults.transformations;
      }

      sourceType.addEventListener('change', () => { sourceConfig.value = defaults.source[sourceType.value]; });
      destinationType.addEventListener('change', () => { destinationConfig.value = defaults.destination[destinationType.value]; });

      async function fetchJson(url, options) {
        const response = await fetch(url, options);
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || 'Request failed');
        return payload;
      }

      function renderSummary(summary) {
        const root = document.getElementById('summary');
        root.textContent = '';
        [['Pipelines', summary.pipeline_count], ['Enabled', summary.enabled_pipeline_count], ['Jobs', summary.job_count], ['Active', summary.active_job_count]].forEach(([label, value]) => {
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

      function renderConnectors(catalog) {
        const root = document.getElementById('connectors');
        root.textContent = '';
        ['sources', 'destinations', 'transformations'].forEach((group) => {
          catalog[group].forEach((item) => {
            const li = document.createElement('li');
            const code = document.createElement('code');
            code.textContent = item.type;
            li.appendChild(code);
            li.appendChild(document.createTextNode(` — ${item.description}`));
            root.appendChild(li);
          });
        });
      }

      function renderPipelines(pipelines) {
        const root = document.getElementById('pipelines');
        root.textContent = '';
        pipelines.forEach((pipeline) => {
          const li = document.createElement('li');
          const title = document.createElement('strong');
          title.textContent = pipeline.name;
          li.appendChild(title);
          const pill = document.createElement('span');
          pill.className = 'pill';
          pill.textContent = ` ${pipeline.source.type} → ${pipeline.destination.type}`;
          li.appendChild(document.createTextNode(' '));
          li.appendChild(pill);
          li.appendChild(document.createElement('br'));
          li.appendChild(document.createTextNode(`status=${pipeline.last_run_status || 'never'} schedule=${pipeline.schedule_interval_seconds || 'manual'}`));
          li.appendChild(document.createElement('br'));
          [['Run now', async () => {
            const job = await fetchJson(`/api/pipelines/${pipeline.id}/run`, { method: 'POST' });
            message.textContent = JSON.stringify(job, null, 2);
          }], ['Preview', async () => {
            const preview = await fetchJson(`/api/pipelines/${pipeline.id}/preview`, { method: 'POST' });
            message.textContent = JSON.stringify(preview, null, 2);
          }, 'secondary'], ['Delete', async () => {
            await fetchJson(`/api/pipelines/${pipeline.id}`, { method: 'DELETE' });
          }, 'danger']].forEach(([label, handler, kind]) => {
            const button = document.createElement('button');
            if (kind) button.className = kind;
            button.textContent = label;
            button.addEventListener('click', async () => {
              try { await handler(); await refresh(); } catch (err) { message.textContent = err.message; }
            });
            li.appendChild(button);
          });
          root.appendChild(li);
        });
      }

      function renderJobs(jobs) {
        const root = document.getElementById('jobs');
        root.textContent = '';
        jobs.forEach((job) => {
          const li = document.createElement('li');
          const title = document.createElement('strong');
          title.textContent = `${job.pipeline_name} — ${job.status}`;
          li.appendChild(title);
          li.appendChild(document.createTextNode(` (${job.trigger})`));
          li.appendChild(document.createElement('br'));
          li.appendChild(document.createTextNode(`source=${job.source_type} destination=${job.destination_type} records=${job.record_count}`));
          if (job.output) {
            const output = document.createElement('pre');
            output.textContent = JSON.stringify(job.output, null, 2);
            li.appendChild(output);
          }
          if (job.error) {
            const error = document.createElement('pre');
            error.textContent = job.error;
            li.appendChild(error);
          }
          if (job.logs && job.logs.length) {
            const logs = document.createElement('pre');
            logs.textContent = job.logs.join('\n');
            li.appendChild(logs);
          }
          root.appendChild(li);
        });
      }

      async function refresh() {
        const [dashboard, catalog, pipelines, jobs] = await Promise.all([
          fetchJson('/api/dashboard'),
          fetchJson('/api/connectors'),
          fetchJson('/api/pipelines'),
          fetchJson('/api/jobs')
        ]);
        renderSummary(dashboard);
        renderConnectors(catalog);
        renderPipelines(pipelines.pipelines);
        renderJobs(jobs.jobs);
      }

      document.getElementById('create').addEventListener('click', async () => {
        try {
          const payload = {
            name: document.getElementById('name').value,
            enabled: true,
            source: { type: sourceType.value, config: JSON.parse(sourceConfig.value) },
            transformations: JSON.parse(transformations.value),
            destination: { type: destinationType.value, config: JSON.parse(destinationConfig.value) }
          };
          const scheduleRaw = document.getElementById('scheduleInterval').value.trim();
          if (scheduleRaw) payload.schedule_interval_seconds = Number(scheduleRaw);
          const pipeline = await fetchJson('/api/pipelines', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          message.textContent = JSON.stringify(pipeline, null, 2);
          await refresh();
        } catch (err) {
          message.textContent = err.message;
        }
      });

      document.getElementById('previewPayload').addEventListener('click', () => {
        try {
          message.textContent = JSON.stringify({
            name: document.getElementById('name').value,
            source: { type: sourceType.value, config: JSON.parse(sourceConfig.value) },
            transformations: JSON.parse(transformations.value),
            destination: { type: destinationType.value, config: JSON.parse(destinationConfig.value) }
          }, null, 2);
        } catch (err) {
          message.textContent = err.message;
        }
      });

      document.getElementById('saveFile').addEventListener('click', async () => {
        try {
          const result = await fetchJson('/api/files', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path: document.getElementById('filePath').value, content: document.getElementById('fileContent').value })
          });
          message.textContent = JSON.stringify(result, null, 2);
        } catch (err) {
          message.textContent = err.message;
        }
      });

      setDefaults();
      refresh();
      setInterval(refresh, 2000);
    </script>
  </body>
</html>
"""


class ManagedHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, handler_class, platform, worker=None, scheduler=None):
        super().__init__(server_address, handler_class)
        self.platform = platform
        self.worker = worker
        self.scheduler = scheduler

    def server_close(self):
        if self.worker:
            self.worker.stop()
        if self.scheduler:
            self.scheduler.stop()
        super().server_close()


def build_handler(platform):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            if parsed.path == "/":
                self._html(200, INDEX_HTML)
                return
            if parsed.path == "/api/health":
                self._json(200, {"status": "ok"})
                return
            if parsed.path == "/api/dashboard":
                self._json(200, platform.get_dashboard())
                return
            if parsed.path == "/api/connectors":
                self._json(200, platform.catalog())
                return
            if len(parts) == 5 and parts[:3] == ["api", "connectors", "source"] and parts[4] == "spec":
                self._json(200, {"type": parts[3], "spec": platform.source_spec(parts[3])})
                return
            if len(parts) == 5 and parts[:3] == ["api", "connectors", "destination"] and parts[4] == "spec":
                self._json(200, {"type": parts[3], "spec": platform.destination_spec(parts[3])})
                return
            if parsed.path == "/api/pipelines":
                self._json(200, {"pipelines": platform.list_pipelines()})
                return
            if len(parts) == 3 and parts[:2] == ["api", "pipelines"]:
                pipeline = platform.get_pipeline(parts[2])
                if pipeline is None:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                self._json(200, pipeline)
                return
            if parsed.path == "/api/jobs":
                self._json(200, {"jobs": platform.list_jobs()})
                return
            if len(parts) == 3 and parts[:2] == ["api", "jobs"]:
                job = platform.get_job(parts[2])
                if job is None:
                    self._json(404, {"error": "Job not found."})
                    return
                self._json(200, job)
                return
            if parsed.path == "/api/demo/contacts":
                self._json(200, {"records": [
                    {"id": 1, "name": "Ada", "email": "ada@example.com", "country": "uk"},
                    {"id": 2, "name": "Grace", "email": "grace@example.com", "country": "us"},
                    {"id": 3, "name": "Linus", "email": "linus@example.com", "country": "fi"}
                ]})
                return
            self._json(404, {"error": "Not found."})

        def do_POST(self):
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]

            if parsed.path == "/api/pipelines":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                try:
                    self._json(201, platform.create_pipeline(payload))
                except ValueError as exc:
                    self._json(400, {"error": str(exc)})
                return

            if parsed.path == "/api/files":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                try:
                    output_path = platform.write_text_file(payload.get("path"), payload.get("content"))
                    self._json(201, {"path": str(output_path)})
                except (ValueError, PipelineExecutionError) as exc:
                    self._json(400, {"error": str(exc)})
                return

            if len(parts) == 5 and parts[:3] == ["api", "connectors", "source"] and parts[4] == "check":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                self._json(200, platform.check_source(parts[3], payload))
                return

            if len(parts) == 5 and parts[:3] == ["api", "connectors", "destination"] and parts[4] == "check":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                self._json(200, platform.check_destination(parts[3], payload))
                return

            if len(parts) == 5 and parts[:3] == ["api", "connectors", "source"] and parts[4] == "discover":
                payload = self._read_json(required=True)
                if payload is None:
                    return
                try:
                    self._json(200, platform.discover_source(parts[3], payload))
                except (PipelineExecutionError, ValueError) as exc:
                    self._json(400, {"error": str(exc)})
                return

            if len(parts) == 4 and parts[:2] == ["api", "pipelines"] and parts[3] == "run":
                try:
                    self._json(202, platform.enqueue_pipeline_run(parts[2], trigger="manual"))
                except KeyError:
                    self._json(404, {"error": "Pipeline not found."})
                except PipelineBusyError as exc:
                    self._json(409, {"error": str(exc)})
                return

            if len(parts) == 4 and parts[:2] == ["api", "pipelines"] and parts[3] == "preview":
                payload = self._read_json(required=False) or {}
                limit = payload.get("limit", 20)
                try:
                    self._json(200, platform.preview_pipeline(parts[2], limit=limit))
                except KeyError:
                    self._json(404, {"error": "Pipeline not found."})
                except PipelineExecutionError as exc:
                    self._json(400, {"error": str(exc)})
                return

            self._json(404, {"error": "Not found."})

        def do_DELETE(self):
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) == 3 and parts[:2] == ["api", "pipelines"]:
                pipeline = platform.delete_pipeline(parts[2])
                if pipeline is None:
                    self._json(404, {"error": "Pipeline not found."})
                    return
                self._json(200, {"deleted": pipeline["id"]})
                return
            self._json(404, {"error": "Not found."})

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
                return json.loads(self.rfile.read(length).decode("utf-8"))
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

        def log_message(self, format, *args):  # noqa: A003
            return

    return Handler


def create_server(host, port, platform, worker=None, scheduler=None):
    return ManagedHTTPServer((host, port), build_handler(platform), platform, worker=worker, scheduler=scheduler)
