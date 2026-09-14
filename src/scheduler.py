import threading


class SchedulerRunner:
    def __init__(self, platform, poll_interval=1.0):
        self.platform = platform
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread = None

    def process_once(self):
        return self.platform.run_due_schedules()

    def serve_forever(self):
        while not self._stop_event.is_set():
            self.process_once()
            self._stop_event.wait(self.poll_interval)

    def start_in_thread(self):
        if self._thread and self._thread.is_alive():
            return self._thread
        self._stop_event.clear()
        self._thread = threading.Thread(target=self.serve_forever, name="etl-scheduler", daemon=True)
        self._thread.start()
        return self._thread

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
