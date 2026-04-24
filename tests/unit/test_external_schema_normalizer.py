"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from fastapi import status
from types import SimpleNamespace

from karapace.core.config import Config
from karapace.core.external_schema_normalizer import ExternalAvroSchemaNormalizer, ExternalSchemaNormalizerError
from unittest.mock import AsyncMock, Mock

import pytest


class _MockResponseContext:
    def __init__(self, response: object) -> None:
        self._response = response

    async def __aenter__(self) -> object:
        return self._response

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.fixture(name="enabled_config")
def fixture_enabled_config() -> Config:
    config = Config()
    config.external_avro_normalizer_global_enabled = True
    config.external_avro_normalizer_url = "http://localhost:8085/api/v2/schemas/normalize"
    config.external_avro_normalizer_timeout_ms = 3000
    return config


async def test_normalize_schema_success(enabled_config: Config) -> None:
    normalizer = ExternalAvroSchemaNormalizer(config=enabled_config)

    response = SimpleNamespace(status=200)
    response.text = AsyncMock(
        return_value='{"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"Order\\"}","normalized":true,"details":null}'
    )
    normalizer._client = Mock()
    normalizer._client.post.return_value = _MockResponseContext(response)

    normalized = await normalizer.normalize_schema(subject="orders-value", schema_str='{"type":"record","name":"Order"}')
    assert normalized == '{"type":"record","name":"Order"}'


async def test_normalize_schema_propagates_upstream_error(enabled_config: Config) -> None:
    normalizer = ExternalAvroSchemaNormalizer(config=enabled_config)

    response = SimpleNamespace(status=400)
    response.text = AsyncMock(return_value='{"error_code":"bad_request","message":"subject is required","details":null}')
    normalizer._client = Mock()
    normalizer._client.post.return_value = _MockResponseContext(response)

    with pytest.raises(ExternalSchemaNormalizerError) as exc_info:
        await normalizer.normalize_schema(subject="", schema_str="{}")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["error_code"] == "bad_request"
    assert exc_info.value.detail["message"] == "subject is required"


async def test_normalize_schema_requires_url_when_enabled(enabled_config: Config) -> None:
    enabled_config.external_avro_normalizer_url = None
    normalizer = ExternalAvroSchemaNormalizer(config=enabled_config)

    with pytest.raises(ExternalSchemaNormalizerError) as exc_info:
        await normalizer.normalize_schema(subject="orders-value", schema_str="{}")

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail["error_code"] == "external_avro_normalizer_misconfigured"


async def test_normalize_schema_validates_success_payload(enabled_config: Config) -> None:
    normalizer = ExternalAvroSchemaNormalizer(config=enabled_config)

    response = SimpleNamespace(status=200)
    response.text = AsyncMock(return_value='{"normalized":true}')
    normalizer._client = Mock()
    normalizer._client.post.return_value = _MockResponseContext(response)

    with pytest.raises(ExternalSchemaNormalizerError) as exc_info:
        await normalizer.normalize_schema(subject="orders-value", schema_str="{}")

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["error_code"] == "external_avro_normalizer_invalid_response"


@pytest.mark.parametrize(
    ("upstream_status", "response_body", "expected_status", "expected_error_code"),
    [
        (
            status.HTTP_400_BAD_REQUEST,
            '{"error_code":"invalid_schema","message":"Invalid schema payload"}',
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            42201,
        ),
        (
            status.HTTP_400_BAD_REQUEST,
            '{"error_code":"bad_request","message":"subject is required"}',
            status.HTTP_400_BAD_REQUEST,
            "bad_request",
        ),
        (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            '{"error_code":"upstream_validation_error","message":"Schema validation failed"}',
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            42201,
        ),
        (
            status.HTTP_503_SERVICE_UNAVAILABLE,
            '{"error_code":"service_unavailable","message":"upstream unavailable"}',
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
    ],
)
async def test_normalize_schema_error_mapping_matrix(
    enabled_config: Config,
    upstream_status: int,
    response_body: str,
    expected_status: int,
    expected_error_code: str | int,
) -> None:
    normalizer = ExternalAvroSchemaNormalizer(config=enabled_config)

    response = SimpleNamespace(status=upstream_status)
    response.text = AsyncMock(return_value=response_body)
    normalizer._client = Mock()
    normalizer._client.post.return_value = _MockResponseContext(response)

    with pytest.raises(ExternalSchemaNormalizerError) as exc_info:
        await normalizer.normalize_schema(subject="orders-value", schema_str="{}")

    assert exc_info.value.status_code == expected_status
    assert exc_info.value.detail["error_code"] == expected_error_code


async def test_normalize_schema_non_json_error_body_uses_fallback(enabled_config: Config) -> None:
    normalizer = ExternalAvroSchemaNormalizer(config=enabled_config)

    response = SimpleNamespace(status=status.HTTP_400_BAD_REQUEST)
    response.text = AsyncMock(return_value="not-json-error")
    normalizer._client = Mock()
    normalizer._client.post.return_value = _MockResponseContext(response)

    with pytest.raises(ExternalSchemaNormalizerError) as exc_info:
        await normalizer.normalize_schema(subject="orders-value", schema_str="{}")

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == {"error_code": status.HTTP_400_BAD_REQUEST, "message": "not-json-error"}
