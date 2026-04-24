"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from karapace.core.config import Config
from karapace.core.serialization import SchemaRetrievalError
from karapace.core.typing import SubjectType
from karapace.kafka_rest_apis import UserRestProxy
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.rapu import HTTPResponse, JSON_CONTENT_TYPE
from unittest.mock import AsyncMock, Mock

import pytest


def _proxy() -> UserRestProxy:
    return UserRestProxy(
        config=Config(),
        kafka_timeout=10,
        serializer=Mock(),
        verify_connection=False,
    )


async def test_validate_schema_info_maps_incompatible_schema_to_conflict() -> None:
    proxy = _proxy()
    proxy.get_schema_id = AsyncMock(
        side_effect=SchemaRetrievalError(
            {
                "error_code": 409,
                "message": "Schema being registered is incompatible with an earlier schema for subject "
                '"orders-value", details: ...',
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

    assert exc_info.value.status == 409
    assert exc_info.value.json["error_code"] == RESTErrorCodes.INCOMPATIBLE_SCHEMA.value
    assert (
        exc_info.value.json["message"]
        == 'Schema being registered is incompatible with an earlier schema for subject "orders-value"'
    )


async def test_validate_schema_info_keeps_schema_retrieval_error_for_non_incompatible_payload() -> None:
    proxy = _proxy()
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
    assert exc_info.value.status != 409
    assert exc_info.value.json["error_code"] == RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value
    assert exc_info.value.json["error_code"] != RESTErrorCodes.INCOMPATIBLE_SCHEMA.value
    assert "Error when registering schema" in exc_info.value.json["message"]
