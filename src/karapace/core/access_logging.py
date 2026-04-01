"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiohttp.web_log import AccessLogger
from aiohttp.web_request import BaseRequest
from aiohttp.web_response import StreamResponse
import logging

HEALTH_PATH = "/_health"


def should_skip_access_log(*, path: str | None, suppress_health_access_logs: bool) -> bool:
    return bool(suppress_health_access_logs and path == HEALTH_PATH)


def extract_path_from_uvicorn_access_record(record: logging.LogRecord) -> str | None:
    args = record.args
    if isinstance(args, tuple) and len(args) >= 3:
        path = args[2]
        if isinstance(path, str):
            return path

    message = record.getMessage()
    if f" {HEALTH_PATH} " in message or f'"{HEALTH_PATH}"' in message:
        return HEALTH_PATH
    return None


class HealthRouteFilter(logging.Filter):
    def __init__(self, *, suppress_health_access_logs: bool) -> None:
        super().__init__()
        self.suppress_health_access_logs = suppress_health_access_logs

    def filter(self, record: logging.LogRecord) -> bool:
        path = extract_path_from_uvicorn_access_record(record)
        return not should_skip_access_log(path=path, suppress_health_access_logs=self.suppress_health_access_logs)


class HealthSkippingAccessLogger(AccessLogger):
    def log(self, request: BaseRequest, response: StreamResponse, time: float) -> None:
        if should_skip_access_log(path=request.path, suppress_health_access_logs=True):
            return
        super().log(request, response, time)
