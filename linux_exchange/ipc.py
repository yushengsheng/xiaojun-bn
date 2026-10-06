"""Bounded local RPC; the worker socket is never exposed over TCP."""
import json
import os
import socket

MAX_MESSAGE = 8 * 1024 * 1024


class RemoteError(RuntimeError):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def receive(stream):
    line = stream.readline(MAX_MESSAGE + 1)
    if len(line) > MAX_MESSAGE or not line.endswith(b"\n"):
        raise ValueError("请求长度无效")
    value = json.loads(line)
    if not isinstance(value, dict):
        raise ValueError("请求格式无效")
    return value


def call(method, payload=None, *, token="", csrf="", socket_path=None):
    request = dict(method=method, payload=payload or {}, token=token, csrf=csrf)
    data = json.dumps(request, ensure_ascii=False).encode() + b"\n"
    if len(data) > MAX_MESSAGE:
        raise RemoteError("请求过长", 413)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(40)
        connection.connect(socket_path or os.environ["XIAOJUN_WORKER_SOCKET"])
        connection.sendall(data)
        with connection.makefile("rb") as stream:
            result = receive(stream)
    if not result.get("ok"):
        raise RemoteError(result.get("error", "服务请求失败"), result.get("code", 400))
    return result.get("result")
