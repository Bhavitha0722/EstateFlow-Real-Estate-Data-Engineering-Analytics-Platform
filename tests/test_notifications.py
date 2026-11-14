import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import dags.notifications as notifications


class RecordingHandler(BaseHTTPRequestHandler):
    payloads = []

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        self.__class__.payloads.append(json.loads(self.rfile.read(length)))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_args):
        return


def start_receiver():
    RecordingHandler.payloads = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), RecordingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_failure_callback_sends_slack_compatible_payload(monkeypatch):
    server = start_receiver()
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/alerts")

    class DagRun:
        dag_id = "real_estate_analytics"
        run_id = "manual__controlled_failure"

    class TaskInstance:
        task_id = "controlled_failure"

    try:
        notifications.notify_pipeline_failure(
            {
                "dag_run": DagRun(),
                "task_instance": TaskInstance(),
                "exception": RuntimeError("controlled test failure"),
            }
        )
    finally:
        server.shutdown()
        server.server_close()

    assert RecordingHandler.payloads == [
        {
            "text": (
                "Airflow pipeline failure: real_estate_analytics\n"
                "Task: controlled_failure\n"
                "Run: manual__controlled_failure\n"
                "Error: controlled test failure"
            )
        }
    ]


def test_successful_task_context_sends_no_notification(monkeypatch):
    server = start_receiver()
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/alerts")

    try:
        notifications.notify_pipeline_failure({"state": "success"})
    finally:
        server.shutdown()
        server.server_close()

    assert RecordingHandler.payloads == []


def test_stale_data_alert_sends_slack_compatible_payload(monkeypatch):
    server = start_receiver()
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/alerts")

    try:
        notifications.notify_stale_data(
            data_loaded_at="2021-01-01T00:00:00+00:00",
            threshold_hours=24,
        )
    finally:
        server.shutdown()
        server.server_close()

    assert RecordingHandler.payloads == [
        {
            "text": (
                "Data freshness alert: real_estate_analytics dataset is stale.\n"
                "Last loaded: 2021-01-01T00:00:00+00:00\n"
                "Threshold: 24 hours\n"
                "Current age: greater than 24 hours"
            )
        }
    ]
