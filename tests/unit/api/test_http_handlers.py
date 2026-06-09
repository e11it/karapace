"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from karapace.api.http_handlers import _json_safe_validation_errors, setup_exception_handlers
from starlette.requests import Request

import asyncio
import json


def _request() -> Request:
    return Request({"type": "http", "method": "POST", "headers": [], "path": "/", "query_string": b""})


def test_json_safe_validation_errors_encodes_bytes() -> None:
    errors = [
        {
            "type": "json_invalid",
            "loc": ("body", 0),
            "msg": "JSON decode error",
            "input": b"\xff\xfe not valid utf-8",
            "ctx": {"error": "Expecting value"},
        }
    ]

    encoded = _json_safe_validation_errors(errors)

    json.dumps(encoded)  # must be JSON serializable
    assert encoded[0]["input"] == "�� not valid utf-8"


def test_validation_exception_handler_survives_bytes_input() -> None:
    """Regression test: bytes in RequestValidationError.errors() must not cause a 500."""
    app = FastAPI()
    setup_exception_handlers(app)
    handler = app.exception_handlers[RequestValidationError]
    exc = RequestValidationError(
        errors=[
            {
                "type": "json_invalid",
                "loc": ("body", 0),
                "msg": "JSON decode error",
                "input": b"\xff\xfe",
                "ctx": {"error": "Expecting value"},
            }
        ]
    )

    response = asyncio.run(handler(_request(), exc))

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    body = json.loads(response.body)
    assert body["error_code"] == 422
    assert body["message"][0]["input"] == "��"
