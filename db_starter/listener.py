import time
import logging
import threading
import socketserver

from .starter import Starter

log = logging.getLogger(__name__)


def listen(host: str, port: int, starter: Starter):
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            log.info('Connection from %s', '{}:{}'.format(*self.client_address))

            engage_thread = threading.Thread(target=starter.engage, name='engage_thread')
            engage_thread.start()

            time.sleep(1)

            self.request.sendall(str(starter.state.value).encode())

    with socketserver.TCPServer((host, port), Handler) as server:
        log.info('Listening on %s:%d...', host, port)
        server.serve_forever()
