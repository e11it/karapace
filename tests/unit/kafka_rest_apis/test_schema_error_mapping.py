"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from karapace.core.serialization import SchemaRetrievalError
from karapace.core.config import Config
from karapace.core.typing import SubjectType
from karapace.kafka_rest_apis import UserRestProxy
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.rapu import HTTPResponse, JSON_CONTENT_TYPE
from unittest.mock import AsyncMock, Mock

import pytest


def _proxy(*, normalizer_enabled: bool) -> UserRestProxy:
    config = Config()
    config.external_avro_normalizer_global_enabled = normalizer_enabled
    return UserRestProxy(
        config=config,
        kafka_timeout=10,
        serializer=Mock(),
        verify_connection=False,
    )


async def test_validate_schema_info_maps_invalid_schema_error_to_unprocessable_entity() -> None:
    proxy = _proxy(normalizer_enabled=True)
    proxy.get_schema_id = AsyncMock(
        side_effect=SchemaRetrievalError(
            {
                "error_code": 42201,
                "message": "Schema validation failed: field `name` is required",
            }
        )
    )

    with pytest.raises(HTTPResponse) as exc_info:
        await proxy.validate_schema_info(
            data={"value_schema": "{}"},
            subject_type=SubjectType.value,
            content_type=JSON_CONTENT_TYPE,
            topic="orders",
            schema_type="avro",
        )

    assert exc_info.value.status == 422
    assert exc_info.value.json["error_code"] == RESTErrorCodes.INVALID_SCHEMA.value
    assert "Schema validation failed" in exc_info.value.json["message"]


async def test_validate_schema_info_keeps_schema_retrieval_error_for_malformed_upstream_payload() -> None:
    proxy = _proxy(normalizer_enabled=True)
    proxy.get_schema_id = AsyncMock(
        side_effect=SchemaRetrievalError(
            {
                "error_code": "bad_request",
                "message": "Malformed request payload or envelope",
            }
        )
    )

    with pytest.raises(HTTPResponse) as exc_info:
        await proxy.validate_schema_info(
            data={"value_schema": "{}"},
            subject_type=SubjectType.value,
            content_type=JSON_CONTENT_TYPE,
            topic="orders",
            schema_type="avro",
        )

    assert exc_info.value.status == 408
    assert exc_info.value.json["error_code"] == RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value
    assert "Error when registering schema" in exc_info.value.json["message"]


async def test_validate_schema_info_keeps_backward_compatibility_when_normalizer_disabled() -> None:
    proxy = _proxy(normalizer_enabled=False)
    proxy.get_schema_id = AsyncMock(
        side_effect=SchemaRetrievalError(
            {
                "error_code": 42201,
                "message": "Schema validation failed",
            }
        )
    )

    with pytest.raises(HTTPResponse) as exc_info:
        await proxy.validate_schema_info(
            data={"value_schema": "{}"},
            subject_type=SubjectType.value,
            content_type=JSON_CONTENT_TYPE,
            topic="orders",
            schema_type="avro",
        )

    assert exc_info.value.status == 408
    assert exc_info.value.json["error_code"] == RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value
