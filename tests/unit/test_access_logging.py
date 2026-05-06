"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from types import SimpleNamespace
from unittest.mock import Mock

from aiohttp.web_log import AccessLogger
from karapace.core.access_logging import (
    HEALTH_PATH,
    HealthRouteFilter,
    HealthSkippingAccessLogger,
    extract_path_from_uvicorn_access_record,
    should_skip_access_log,
)
from karapace.core.config import Config
from karapace.core.logging_setup import configure_uvicorn_access_logging
from karapace.core.utils import DebugAccessLogger, HealthSkippingDebugAccessLogger

import logging


def test_should_skip_access_log() -> None:
    assert should_skip_access_log(path=HEALTH_PATH, suppress_health_access_logs=True)
    assert not should_skip_access_log(path="/topics", suppress_health_access_logs=True)
    assert not should_skip_access_log(path=HEALTH_PATH, suppress_health_access_logs=False)


def test_extract_path_from_uvicorn_access_record_from_args() -> None:
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:0", "GET", HEALTH_PATH, "1.1", 200),
        exc_info=None,
    )
    assert extract_path_from_uvicorn_access_record(record) == HEALTH_PATH


def test_health_route_filter_drops_health_logs() -> None:
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:0", "GET", HEALTH_PATH, "1.1", 200),
        exc_info=None,
    )
    assert HealthRouteFilter(suppress_health_access_logs=True).filter(record) is False


def test_configure_uvicorn_access_logging_adds_filter() -> None:
    logger = logging.getLogger("uvicorn.access")
    logger.filters.clear()
    configure_uvicorn_access_logging(config=Config(suppress_health_access_logs=True))
    assert any(isinstance(log_filter, HealthRouteFilter) for log_filter in logger.filters)
    logger.filters.clear()


def test_health_skipping_access_logger_short_circuit(monkeypatch) -> None:
    called = Mock()
    monkeypatch.setattr(AccessLogger, "log", called)

    logger = HealthSkippingAccessLogger.__new__(HealthSkippingAccessLogger)
    logger.log(SimpleNamespace(path=HEALTH_PATH), SimpleNamespace(), 0.0)
    called.assert_not_called()


def test_health_skipping_debug_access_logger_short_circuit(monkeypatch) -> None:
    called = Mock()
    monkeypatch.setattr(DebugAccessLogger, "log", called)

    logger = HealthSkippingDebugAccessLogger.__new__(HealthSkippingDebugAccessLogger)
    logger.log(SimpleNamespace(path=HEALTH_PATH), SimpleNamespace(), 0.0)
    called.assert_not_called()
