"""Server-push WebSocket on a BaseHTTPRequestHandler socket over websockets' sans-I/O ServerProtocol."""
from __future__ import annotations

import select

from websockets.datastructures import Headers
from websockets.frames import OP_CLOSE, OP_PING, OP_PONG
from websockets.http11 import Request
from websockets.protocol import State
from websockets.server import ServerProtocol

CLOSE_NOT_ENTITLED = 4403


class WebSocketPush:
    def __init__(self, handler) -> None:
        self._sock = handler.request
        self._protocol = ServerProtocol(max_size=4096)
        self._request = Request(handler.path, Headers(handler.headers.items()))
        self.open = False

    def accept(self) -> int:
        response = self._protocol.accept(self._request)
        self._protocol.send_response(response)
        self._flush()
        self.open = response.status_code == 101
        return response.status_code

    def send_text(self, text: str) -> None:
        self._protocol.send_text(text.encode())
        self._flush()

    def ping(self) -> None:
        self._protocol.send_ping(b"")
        self._flush()

    def close(self, code: int = 1000, reason: str = "") -> None:
        if self._protocol.state is State.OPEN:
            self._protocol.send_close(code, reason)
            self._flush()
        self.open = False

    def poll(self) -> bool:
        while self.open:
            readable, _, _ = select.select([self._sock], [], [], 0)
            if not readable:
                return True
            data = self._sock.recv(4096)
            if not data:
                self._protocol.receive_eof()
                self.open = False
                return False
            self._protocol.receive_data(data)
            for frame in self._protocol.events_received():
                if frame.opcode in (OP_PING, OP_PONG):
                    continue
                if frame.opcode == OP_CLOSE:
                    self._flush()
                    self.open = False
                    return False
                self.close(1003, "server push only")
                return False
            self._flush()
        return False

    def _flush(self) -> None:
        for chunk in self._protocol.data_to_send():
            if chunk:
                self._sock.sendall(chunk)
