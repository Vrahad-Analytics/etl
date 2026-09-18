import os
import sys

from src.api_server import INDEX_HTML, create_server
from src.platform_service import PipelineBusyError, PipelineExecutionError, PlatformService
from src.scheduler import SchedulerRunner
from src.worker import WorkerRunner


DEFAULT_HOST = os.environ.get("ETL_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("ETL_PORT", "8000"))
DEFAULT_DATA_DIR = os.environ.get("ETL_DATA_DIR", "data")


def create_platform(data_dir=DEFAULT_DATA_DIR):
    return PlatformService(data_dir)


def create_runtime(platform, embedded=True):
    worker = WorkerRunner(platform)
    scheduler = SchedulerRunner(platform)
    if embedded:
        worker.start_in_thread()
        scheduler.start_in_thread()
    return worker, scheduler


def create_managed_server(host=DEFAULT_HOST, port=DEFAULT_PORT, data_dir=DEFAULT_DATA_DIR, embedded_runtime=True):
    platform = create_platform(data_dir)
    worker, scheduler = create_runtime(platform, embedded=embedded_runtime)
    if not embedded_runtime:
        worker = None
        scheduler = None
    server = create_server(host, port, platform, worker=worker, scheduler=scheduler)
    return server


def run_api(host=DEFAULT_HOST, port=DEFAULT_PORT, data_dir=DEFAULT_DATA_DIR, embedded_runtime=True):
    server = create_managed_server(host=host, port=port, data_dir=data_dir, embedded_runtime=embedded_runtime)
    print(f"Serving ETL platform at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def run_worker(data_dir=DEFAULT_DATA_DIR):
    platform = create_platform(data_dir)
    worker = WorkerRunner(platform)
    worker.serve_forever()


def run_scheduler(data_dir=DEFAULT_DATA_DIR):
    platform = create_platform(data_dir)
    scheduler = SchedulerRunner(platform)
    scheduler.serve_forever()


def main(argv=None):
    argv = list(argv or sys.argv[1:])
    mode = argv[0] if argv else "all"
    if mode == "all":
        run_api(embedded_runtime=True)
        return
    if mode == "api":
        run_api(embedded_runtime=False)
        return
    if mode == "worker":
        run_worker()
        return
    if mode == "scheduler":
        run_scheduler()
        return
    raise SystemExit(f"Unknown mode: {mode}")


if __name__ == "__main__":
    main()
