import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

API_BASE = os.getenv("API_BASE", "http://localhost:8000")
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_ROOT = Path(__file__).parent
OPERATIONS_API_KEY = os.getenv("OPERATIONS_API_KEY", "")

if not OPERATIONS_API_KEY:
    env_path = DASHBOARD_ROOT.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "OPERATIONS_API_KEY":
                OPERATIONS_API_KEY = value.strip()
                break


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_ROOT, **kwargs)

    def do_GET(self):
        if self.path == "/operations/status":
            self.proxy_api_request("/api/v1/operations/pipeline")
            return
        if self.path == "/operations/history":
            self.proxy_api_request("/api/v1/operations/pipeline/history")
            return
        if self.path.startswith("/api/") or self.path.startswith("/health/"):
            self.proxy_api_request()
            return
        super().do_GET()

    def do_POST(self):
        if self.path == "/operations/run":
            self.proxy_api_request("/api/v1/operations/pipeline/run", "POST")
            return
        if self.path.startswith("/operations/retry"):
            query = self.path.partition("?")[2]
            target = "/api/v1/operations/pipeline/retry"
            if query:
                target = f"{target}?{query}"
            self.proxy_api_request(target, "POST")
            return
        self.send_error(404)

    def end_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        super().end_headers()

    def proxy_api_request(self, target_path=None, method="GET"):
        try:
            headers = {"Accept": "application/json"}
            if target_path and OPERATIONS_API_KEY:
                headers["X-Operations-Key"] = OPERATIONS_API_KEY
            request = Request(
                f"{API_BASE}{target_path or self.path}",
                headers=headers,
                method=method,
            )
            with urlopen(request, timeout=10) as response:
                body = response.read()
                self.send_response(response.status)
                content_type = response.headers.get("Content-Type", "application/json")
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except URLError as error:
            body = b'{"detail":"Analytics API unavailable"}'
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            print(f"API proxy error: {error}")


if __name__ == "__main__":
    server = ThreadingHTTPServer((DASHBOARD_HOST, 5173), DashboardHandler)
    print(f"Atlas dashboard running at http://{DASHBOARD_HOST}:5173")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard")
        server.server_close()
