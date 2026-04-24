"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from fastapi.exceptions import HTTPException

from karapace.api.container import SchemaRegistryContainer
from karapace.api.routers.requests import SchemaRequest
from karapace.core.external_schema_normalizer import ExternalSchemaNormalizerError
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from karapace.core.schema_reader import KafkaSchemaReader
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import PrimaryInfo
from karapace.rapu import HTTPResponse
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import asyncio
import json
import pytest


TYPED_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.data",
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "attr1",
                    "type": ["null", "string"],
                }
            ],
        }
    ),
)


async def test_validate_schema_request_body(schema_registry_container: SchemaRegistryContainer) -> None:
    schema_registry_container.schema_registry_controller()._validate_schema_type(
        {"schema": "{}", "schemaType": "JSON", "references": [], "metadata": {}, "ruleSet": {}}
    )

    with pytest.raises(HTTPException) as exc_info:
        schema_registry_container.schema_registry_controller()._validate_schema_type(
            {"schema": "{}", "schemaType": "DOES_NOT_EXIST", "references": [], "unexpected_field_name": {}, "ruleSet": {}},
        )
    assert exc_info.type is HTTPException
    assert str(exc_info.value) == "422: {'error_code': 422, 'message': 'Invalid schemaType DOES_NOT_EXIST'}"


async def test_forward_when_not_ready(schema_registry_container: SchemaRegistryContainer) -> None:
    with patch("karapace.api.container.KarapaceSchemaRegistry") as schema_registry_class:
        schema_reader_mock = Mock(spec=KafkaSchemaReader)
        schema_registry_mock = Mock(spec=KarapaceSchemaRegistry)
        ready_property_mock = PropertyMock(return_value=False)
        type(schema_reader_mock).ready = ready_property_mock
        schema_registry_class.schema_reader = schema_reader_mock

        schema_registry_class.schemas_get.return_value = TYPED_AVRO_SCHEMA
        schema_registry_mock.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://primary-url")

        close_future_result = asyncio.Future()
        close_future_result.set_result(True)
        close_func = Mock()
        close_func.return_value = close_future_result
        schema_registry_class.close = close_func

        controller = schema_registry_container.schema_registry_controller()
        controller.schema_registry = schema_registry_class

        mock_forward_func_future = asyncio.Future()
        mock_forward_func_future.set_exception(HTTPResponse({"mock": "response"}))
        mock_forward_func = Mock()
        mock_forward_func.return_value = mock_forward_func_future
        controller._forward_request_remote = mock_forward_func

        assert await controller.schemas_get(
            schema_id=1,
            include_subjects=False,
            fetch_max_id=False,
            format_serialized="",
            user=None,
            authorizer=None,
        )
        with pytest.raises(HTTPResponse):
            # prevent `future exception was never retrieved` warning logs
            # future: <Future finished exception=HTTPResponse(status=200 body={'mock': 'response'})>
            await mock_forward_func_future


async def test_external_normalizer_bypass_when_disabled(schema_registry_container: SchemaRegistryContainer) -> None:
    controller = schema_registry_container.schema_registry_controller()
    controller.external_avro_schema_normalizer = Mock(enabled=False, normalize_schema=AsyncMock())
    schema_request = SchemaRequest(schema="{}", schemaType=SchemaType.AVRO.value)

    result = await controller._normalize_schema_request_if_needed(subject="orders-value", schema_request=schema_request)

    assert result is schema_request
    controller.external_avro_schema_normalizer.normalize_schema.assert_not_called()


async def test_external_normalizer_applies_to_avro(schema_registry_container: SchemaRegistryContainer) -> None:
    controller = schema_registry_container.schema_registry_controller()
    controller.external_avro_schema_normalizer = Mock(
        enabled=True,
        normalize_schema=AsyncMock(return_value='{"type":"record","name":"Normalized"}'),
    )
    schema_request = SchemaRequest(schema='{"type":"record","name":"Original"}', schemaType=SchemaType.AVRO.value)

    result = await controller._normalize_schema_request_if_needed(subject="orders-value", schema_request=schema_request)

    assert result.schema_str == '{"type":"record","name":"Normalized"}'


async def test_subject_post_propagates_external_normalizer_http_error(
    schema_registry_container: SchemaRegistryContainer,
) -> None:
    controller = schema_registry_container.schema_registry_controller()
    controller.external_avro_schema_normalizer = Mock(
        enabled=True,
        normalize_schema=AsyncMock(
            side_effect=ExternalSchemaNormalizerError(
                status_code=422,
                detail={"error_code": 42201, "message": "Invalid schema payload"},
            )
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await controller.subject_post(
            subject="orders-value",
            schema_request=SchemaRequest(schema="{}", schemaType=SchemaType.AVRO.value),
            normalize=False,
            forward_client=Mock(),
            request=Mock(),
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == 42201


async def test_subjects_schema_post_propagates_external_normalizer_http_error(
    schema_registry_container: SchemaRegistryContainer,
) -> None:
    controller = schema_registry_container.schema_registry_controller()
    controller.external_avro_schema_normalizer = Mock(
        enabled=True,
        normalize_schema=AsyncMock(
            side_effect=ExternalSchemaNormalizerError(
                status_code=422,
                detail={"error_code": 42201, "message": "Invalid schema payload"},
            )
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await controller.subjects_schema_post(
            subject="orders-value",
            schema_request=SchemaRequest(schema="{}", schemaType=SchemaType.AVRO.value),
            deleted=False,
            normalize=False,
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == 42201


async def test_compatibility_check_propagates_external_normalizer_http_error(
    schema_registry_container: SchemaRegistryContainer,
) -> None:
    controller = schema_registry_container.schema_registry_controller()
    controller.external_avro_schema_normalizer = Mock(
        enabled=True,
        normalize_schema=AsyncMock(
            side_effect=ExternalSchemaNormalizerError(
                status_code=422,
                detail={"error_code": 42201, "message": "Invalid schema payload"},
            )
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await controller.compatibility_check(
            subject="orders-value",
            schema_request=SchemaRequest(schema="{}", schemaType=SchemaType.AVRO.value),
            version="latest",
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == 42201
