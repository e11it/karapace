"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Sequence
from fastapi import FastAPI, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from http import HTTPStatus
from karapace.api.routers.errors import KarapaceValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request as StarletteHTTPRequest
from typing import Any


def _json_safe_validation_errors(errors: Sequence[Any]) -> list[Any]:
    return jsonable_encoder(errors, custom_encoder={bytes: lambda value: value.decode("utf-8", errors="replace")})


def setup_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(_: StarletteHTTPRequest, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(_: StarletteHTTPRequest, exc: RequestValidationError) -> JSONResponse:
        error_code = HTTPStatus.UNPROCESSABLE_ENTITY.value
        if isinstance(exc, KarapaceValidationError):
            error_code = exc.error_code
            message = exc.body
        else:
            message = _json_safe_validation_errors(exc.errors())
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error_code": error_code,
                "message": message,
            },
        )
