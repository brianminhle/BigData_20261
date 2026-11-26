"""Local read-only HTTP interface; serves only explicit assets and aggregate API."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import logging
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from airtraffic.serving.query import query_view

ASSETS = {'/': ('index.html', 'text/html; charset=utf-8'),
          '/app.js': ('app.js', 'text/javascript; charset=utf-8'),
          '/style.css': ('style.css', 'text/css; charset=utf-8')}


def handler_for(view_path=None, store=None):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            try:
                if url.path in ('/livez', '/readyz'):
                    if url.path == '/readyz':
                        if store:
                            store.metadata()
                        else:
                            json.loads(Path(view_path).read_text())
                    payload, content_type = b'{"status":"ok"}', 'application/json'
                elif url.path == '/api/overview':
                    params = parse_qs(url.query)
                    result = store.overview(params) if store else query_view(json.loads(Path(view_path).read_text()), params)
                    payload = json.dumps(result, allow_nan=False).encode()
                    content_type = 'application/json'
                elif store and url.path in ('/api/movement', '/api/gaps'):
                    result = store.movement(parse_qs(url.query), gaps_only=url.path == '/api/gaps')
                    payload, content_type = json.dumps(result, allow_nan=False).encode(), 'application/json'
                elif url.path in ASSETS:
                    filename, content_type = ASSETS[url.path]
                    payload = (Path(__file__).parent / 'static' / filename).read_bytes()
                else:
                    self.send_error(404)
                    return
            except ValueError:
                self.send_error(400, 'Invalid query or export')
                return
            except Exception as error:
                logging.error('Serving unavailable (%s)', type(error).__name__)
                self.send_error(503, 'Serving data temporarily unavailable')
                return
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(payload)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.end_headers()
            self.wfile.write(payload)
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--view', default='data/dashboard/view.json')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=18080)
    args = parser.parse_args()
    store = None
    if os.getenv('AIRTRAFFIC_MONGO_URI'):
        from airtraffic.serving.mongo import ServingStore, connect_database
        store = ServingStore(connect_database())
    server = ThreadingHTTPServer((args.host, args.port), handler_for(args.view, store))
    print(f'Dashboard: http://{args.host}:{args.port}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if store:
            store.db.client.close()


if __name__ == '__main__':
    main()
