"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.container import KarapaceContainer
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema, Versioner
from karapace.core.serialization import (
    _schema_needs_logical_conversion,
    convert_logical_types,
    flatten_unions,
    get_subject_name,
    HEADER_FORMAT,
    InvalidMessageHeader,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistryClient,
    SchemaRegistrySerializer,
    SchemaRetrievalError,
    sr_authorization_ctx,
    START_BYTE,
    write_value,
)
from karapace.core.typing import NameStrategy, Subject, SubjectType
from tests.utils import schema_avro_json, test_objects_avro
from unittest.mock import AsyncMock, call, Mock, patch

import asyncio
import avro
import base64
import copy
import datetime
import decimal
import io
import json
import logging
import pytest
import struct

log = logging.getLogger(__name__)

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
                },
                {
                    "name": "attr2",
                    "type": ["null", "string"],
                },
                {
                    "name": "attrArray",
                    "type": ["null", {"type": "array", "items": "string"}],
                },
                {
                    "name": "attrMap",
                    "type": ["null", {"type": "map", "values": "string"}],
                },
                {
                    "name": "attrRecord",
                    "type": ["null", {"type": "record", "name": "Record", "fields": [{"name": "attr1", "type": "string"}]}],
                },
            ],
        }
    ),
)

TYPED_JSON_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.JSONSCHEMA,
    json.dumps(
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Test",
            "type": "object",
            "properties": {"attr1": {"type": ["null", "string"]}, "attr2": {"type": ["null", "string"]}},
        }
    ),
)

TYPED_AVRO_SCHEMA_WITHOUT_NAMESPACE = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "attr1",
                    "type": ["null", "string"],
                },
                {
                    "name": "attr2",
                    "type": ["null", "string"],
                },
            ],
        }
    ),
)

TYPED_PROTOBUF_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.PROTOBUF,
    """\
    syntax = "proto3";

    message Test {
        string attr1 = 1;
        string attr2 = 2;
    }\
    """,
)

NAMESPACED_UNION_SCHEMA = {
    "type": "record",
    "name": "Simple",
    "namespace": "example.avro",
    "fields": [
        {
            "name": "payload",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Payload",
                    "namespace": "org.polyus.ipl.ds.erd.doc.asdfasdf.ver1",
                    "fields": [{"name": "amount", "type": "float"}],
                },
            ],
        }
    ],
}

MAP_UNION_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "MapUnionTest",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
            ],
        }
    ),
)

AVRO_BYTES_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.bytes",
            "name": "BytesEnvelope",
            "type": "record",
            "fields": [
                {
                    "name": "payload",
                    "type": {
                        "type": "record",
                        "name": "Payload",
                        "fields": [
                            {"name": "raw", "type": "bytes"},
                            {"name": "items", "type": {"type": "array", "items": "bytes"}},
                        ],
                    },
                }
            ],
        }
    ),
)

COMPLEX_UNION_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "MinimalUnionTest",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "attrs",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Attr",
                            "fields": [
                                {"name": "k", "type": "string"},
                                {"name": "v", "type": ["null", "string", "long", "double", "boolean"]},
                            ],
                        },
                    },
                },
                {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
            ],
        }
    ),
)


async def make_ser_deser(
    karapace_container: KarapaceContainer, mock_client: SchemaRegistryClient
) -> SchemaRegistrySerializer:
    serializer = SchemaRegistrySerializer(config=karapace_container.config())
    await serializer.registry_client.close()
    serializer.registry_client = mock_client
    return serializer


async def test_happy_flow(karapace_container: KarapaceContainer):
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    assert len(serializer.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject(Subject("top"))
    for o in test_objects_avro:
        assert o == await serializer.deserialize(await serializer.serialize(schema, o))
    assert len(serializer.ids_to_schemas) == 1
    assert 1 in serializer.ids_to_schemas

    assert mock_registry_client.method_calls == [call.get_schema("top"), call.get_schema_for_id(1)]


@pytest.mark.parametrize(
    ["record", "flattened_record"],
    [
        [{"attr1": {"string": "sample data"}, "attr2": None}, {"attr1": "sample data", "attr2": None}],
        [{"attr1": None, "attr2": None}, {"attr1": None, "attr2": None}],
        [{"attrArray": {"array": ["item1", "item2"]}}, {"attrArray": ["item1", "item2"]}],
        [{"attrMap": {"map": {"k1": "v1", "k2": "v2"}}}, {"attrMap": {"k1": "v1", "k2": "v2"}}],
        [{"attrRecord": {"Record": {"attr1": "test"}}}, {"attrRecord": {"attr1": "test"}}],
    ],
)
def test_flatten_unions_record(record, flattened_record) -> None:
    assert flatten_unions(TYPED_AVRO_SCHEMA.schema, record) == flattened_record


def test_flatten_unions_record_short_name_is_legacy_compatible() -> None:
    """Keep permissive short-name behavior in flatten_unions for backward compatibility."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(NAMESPACED_UNION_SCHEMA))
    record = {"payload": {"Payload": {"amount": 2.3}}}

    flattened = flatten_unions(typed_schema.schema, record)
    assert flattened == {"payload": {"amount": 2.3}}


def test_flatten_unions_array() -> None:
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        json.dumps(
            {
                "type": "array",
                "items": {
                    "namespace": "io.aiven.data",
                    "name": "Test",
                    "type": "record",
                    "fields": [
                        {
                            "name": "attr",
                            "type": ["null", "string"],
                        }
                    ],
                },
            }
        ),
    )
    record = [{"attr": {"string": "sample data"}}]
    flatten_record = [{"attr": "sample data"}]
    assert flatten_unions(typed_schema.schema, record) == flatten_record

    record = [{"attr": None}]
    assert flatten_unions(typed_schema.schema, record) == record


def test_flatten_unions_map() -> None:
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        json.dumps(
            {
                "type": "map",
                "values": {
                    "namespace": "io.aiven.data",
                    "name": "Test",
                    "type": "record",
                    "fields": [
                        {
                            "name": "attr1",
                            "type": ["null", "string"],
                        }
                    ],
                },
            }
        ),
    )
    record = {"foo": {"attr1": {"string": "sample data"}}}
    flatten_record = {"foo": {"attr1": "sample data"}}
    assert flatten_unions(typed_schema.schema, record) == flatten_record

    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        json.dumps({"type": "array", "items": ["null", "string", "int"]}),
    )
    record = [{"string": "foo"}, None, {"int": 1}]
    flatten_record = ["foo", None, 1]
    assert flatten_unions(typed_schema.schema, record) == flatten_record


@pytest.mark.parametrize(
    "schema_json,value,expected_type",
    (
        ({"type": "long", "logicalType": "timestamp-millis"}, 1_600_000_000_000, "datetime"),
        ({"type": "long", "logicalType": "timestamp-micros"}, 1_600_000_000_000_000, "datetime"),
        ({"type": "int", "logicalType": "date"}, 18_000, "date"),
        ({"type": "int", "logicalType": "time-millis"}, 12 * 60 * 60 * 1000, "time"),
        ({"type": "long", "logicalType": "time-micros"}, 12 * 60 * 60 * 1_000_000, "time"),
        ({"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}, "123.45", "decimal"),
    ),
)
def test_convert_logical_types_primitives(schema_json, value, expected_type) -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    extended = expected_type == "decimal"
    converted = convert_logical_types(typed_schema.schema, value, extended_json_parser=extended)

    if expected_type == "datetime":
        assert isinstance(converted, datetime.datetime)
    elif expected_type == "date":
        assert isinstance(converted, datetime.date)
    elif expected_type == "time":
        assert isinstance(converted, datetime.time)
    elif expected_type == "decimal":
        assert isinstance(converted, decimal.Decimal)


def test_convert_logical_types_in_record_and_union() -> None:
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {
                "name": "ts",
                "type": [
                    "null",
                    {
                        "type": "long",
                        "logicalType": "timestamp-millis",
                    },
                ],
            },
            {
                "name": "d",
                "type": {
                    "type": "int",
                    "logicalType": "date",
                },
            },
        ],
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema))
    value = {"ts": 1_600_000_000_000, "d": 18_000}

    converted = convert_logical_types(typed_schema.schema, value)
    assert isinstance(converted["ts"], datetime.datetime)
    assert isinstance(converted["d"], datetime.date)


def test_convert_logical_types_decimal_quantize_int() -> None:
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 4,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    converted = convert_logical_types(typed_schema.schema, 12345, extended_json_parser=True)

    assert isinstance(converted, decimal.Decimal)
    assert str(converted) == "12345.0000"


def test_convert_logical_types_decimal_scale_overflow_raises() -> None:
    """More fractional digits than the schema scale must be an error, not a silent rounding."""
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 18,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    for extended in (False, True):
        with pytest.raises(InvalidPayload, match="more fractional digits"):
            convert_logical_types(typed_schema.schema, "12345.123", extended_json_parser=extended)


def test_convert_logical_types_decimal_confluent_base64() -> None:
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    converted = convert_logical_types(typed_schema.schema, "BZw=")

    assert isinstance(converted, decimal.Decimal)
    assert str(converted) == "14.36"


def test_convert_logical_types_decimal_invalid_base64() -> None:
    """A string that is neither a number nor valid base64 must raise a clear error."""
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    with pytest.raises(InvalidPayload, match="not a valid decimal value"):
        convert_logical_types(typed_schema.schema, "not-base64!")


def test_convert_logical_types_decimal_numeric_string_default_mode() -> None:
    """Default (Confluent-compatible) mode must round-trip numeric strings produced by consume."""
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    converted = convert_logical_types(typed_schema.schema, "14.36")
    assert converted == decimal.Decimal("14.36")


def test_convert_logical_types_decimal_digit_string_is_number_not_base64() -> None:
    """ "1436" is a valid base64 string, but it must be parsed as the number 1436."""
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    converted = convert_logical_types(typed_schema.schema, "1436")
    assert converted == decimal.Decimal("1436.00")


def test_convert_logical_types_decimal_int_default_mode() -> None:
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    converted = convert_logical_types(typed_schema.schema, 1436)
    assert converted == decimal.Decimal("1436.00")


def test_convert_logical_types_decimal_negative_string_and_base64() -> None:
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    assert convert_logical_types(typed_schema.schema, "-7.5") == decimal.Decimal("-7.50")
    # base64 of two's complement unscaled -750 (b"\xfd\x12")
    assert convert_logical_types(typed_schema.schema, "/RI=") == decimal.Decimal("-7.50")


def test_convert_logical_types_decimal_union_branch_failure_is_not_fatal() -> None:
    """A failing decimal conversion in one union branch must not break other branches."""
    schema_json = [
        {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2},
        "string",
    ]
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    # Neither numeric nor base64: the decimal branch raises internally, the string branch wins.
    assert convert_logical_types(typed_schema.schema, "garbage!") == "garbage!"


def test_convert_logical_types_decimal_float_is_rejected() -> None:
    schema_json = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    value = 14.36
    converted = convert_logical_types(typed_schema.schema, value)
    assert converted == value


def test_convert_logical_types_bytes_base64_string_round_trips() -> None:
    """Consume renders bytes as base64; produce must decode the same string back to the same bytes."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps("bytes"))

    raw = b"\x01\x02"
    consumed = base64.b64encode(raw).decode("ascii")  # "AQI="
    assert convert_logical_types(typed_schema.schema, consumed) == raw


def test_convert_logical_types_bytes_latin1_fallback() -> None:
    """Avro JSON spec strings that are not valid base64 are decoded as latin-1."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps("bytes"))

    assert convert_logical_types(typed_schema.schema, "hello!") == b"hello!"
    assert convert_logical_types(typed_schema.schema, "\x01\x02\xff") == b"\x01\x02\xff"


def test_convert_logical_types_bytes_valid_base64_wins_over_latin1() -> None:
    """A latin-1 string that is also valid base64 is interpreted as base64 (round-trip wins)."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps("bytes"))

    assert convert_logical_types(typed_schema.schema, "abcd") == base64.b64decode("abcd")


def test_convert_logical_types_bytes_empty_string() -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps("bytes"))

    assert convert_logical_types(typed_schema.schema, "") == b""


def test_convert_logical_types_bytes_invalid_string_raises() -> None:
    """A string that fits neither base64 nor latin-1 must raise a clear error."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps("bytes"))

    with pytest.raises(InvalidPayload, match="not a valid bytes value"):
        convert_logical_types(typed_schema.schema, "дата")


def test_convert_logical_types_bytes_union_branch_failure_is_not_fatal() -> None:
    """A failing bytes conversion in one union branch must not break other branches."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(["bytes", "string"]))

    # Neither base64 nor latin-1: the bytes branch raises internally, the string branch wins.
    assert convert_logical_types(typed_schema.schema, "дата") == "дата"
    # Valid base64 resolves to the bytes branch.
    assert convert_logical_types(typed_schema.schema, "AQI=") == b"\x01\x02"


def test_convert_logical_types_fixed_base64_string_round_trips() -> None:
    schema_json = {"type": "fixed", "name": "F", "size": 2}
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    assert convert_logical_types(typed_schema.schema, "AQI=") == b"\x01\x02"


def test_convert_logical_types_fixed_latin1_selected_by_size() -> None:
    """ "AQID" is valid base64 but decodes to 3 bytes; for fixed(4) the latin-1 reading must win."""
    schema_json = {"type": "fixed", "name": "F", "size": 4}
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    assert convert_logical_types(typed_schema.schema, "AQID") == b"AQID"


def test_convert_logical_types_fixed_size_mismatch_raises() -> None:
    schema_json = {"type": "fixed", "name": "F", "size": 5}
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    with pytest.raises(InvalidPayload, match="not a valid fixed value"):
        convert_logical_types(typed_schema.schema, "abc")


def test_convert_logical_types_fixed_decimal_string_is_decimal_not_latin1() -> None:
    """Strings for fixed-backed decimals must go through decimal conversion, not latin-1 encoding."""
    schema_json = {
        "type": "fixed",
        "name": "F",
        "size": 2,
        "logicalType": "decimal",
        "precision": 4,
        "scale": 2,
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    assert convert_logical_types(typed_schema.schema, "14.36") == decimal.Decimal("14.36")


_MILLIS_PER_DAY = 86_400_000
_MICROS_PER_DAY = 86_400_000_000
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    ("schema_json", "expected"),
    [
        ({"type": "record", "name": "R", "fields": [{"name": "a", "type": "int"}]}, False),
        ({"type": "record", "name": "R", "fields": [{"name": "a", "type": ["null", "string"]}]}, False),
        ({"type": "record", "name": "R", "fields": [{"name": "a", "type": "bytes"}]}, True),
        (
            {"type": "record", "name": "R", "fields": [{"name": "a", "type": {"type": "fixed", "name": "F", "size": 4}}]},
            True,
        ),
        (
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "a", "type": {"type": "long", "logicalType": "timestamp-millis"}}],
            },
            True,
        ),
        (
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "a", "type": {"type": "array", "items": {"type": "int", "logicalType": "date"}}}],
            },
            True,
        ),
        (
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"name": "a", "type": {"type": "map", "values": ["null", {"type": "int", "logicalType": "date"}]}}
                ],
            },
            True,
        ),
        ({"type": "array", "items": "string"}, False),
        ({"type": "string", "logicalType": "uuid"}, True),
    ],
)
def test_schema_needs_logical_conversion_detection(schema_json, expected: bool) -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    assert _schema_needs_logical_conversion(typed_schema.schema) is expected
    # The result is memoized on the schema object.
    assert getattr(typed_schema.schema, "_karapace_needs_logical_conversion") is expected


def test_schema_needs_logical_conversion_recursive_schema() -> None:
    """A record referencing itself must not send the schema walk into infinite recursion."""
    plain = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "value", "type": "string"},
            {"name": "next", "type": ["null", "Node"]},
        ],
    }
    typed_plain = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(plain))
    assert _schema_needs_logical_conversion(typed_plain.schema) is False

    with_logical = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "next", "type": ["null", "Node"]},
        ],
    }
    typed_logical = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(with_logical))
    assert _schema_needs_logical_conversion(typed_logical.schema) is True


def test_write_value_skips_conversion_for_plain_schema(karapace_container: KarapaceContainer) -> None:
    """The fast path for schemas without logical types must produce identical bytes."""
    schema_json = {
        "type": "record",
        "name": "Plain",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "maybe", "type": ["null", "string"]},
        ],
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))
    config = karapace_container.config()

    for record in ({"name": "x", "maybe": None}, {"name": "x", "maybe": {"string": "tagged"}}, {"name": "x", "maybe": "y"}):
        with patch("karapace.core.serialization.convert_logical_types") as mock_convert:
            buffer = io.BytesIO()
            write_value(config, typed_schema, buffer, record)
            mock_convert.assert_not_called()
        assert buffer.getvalue()


@pytest.mark.parametrize(
    ("logical_type", "base_type", "value"),
    [
        ("time-millis", "int", -1),
        ("time-millis", "int", _MILLIS_PER_DAY),
        ("time-micros", "long", -1),
        ("time-micros", "long", _MICROS_PER_DAY),
    ],
)
def test_convert_logical_types_time_out_of_range_raises(logical_type: str, base_type: str, value: int) -> None:
    """Out-of-range time values must raise instead of silently wrapping around the day."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps({"type": base_type, "logicalType": logical_type}))
    with pytest.raises(InvalidPayload, match=f"not a valid {logical_type} value"):
        convert_logical_types(typed_schema.schema, value)


@pytest.mark.parametrize(
    ("logical_type", "base_type", "value", "expected"),
    [
        ("time-millis", "int", 0, datetime.time(0, 0, 0)),
        ("time-millis", "int", _MILLIS_PER_DAY - 1, datetime.time(23, 59, 59, 999000)),
        ("time-micros", "long", 0, datetime.time(0, 0, 0)),
        ("time-micros", "long", _MICROS_PER_DAY - 1, datetime.time(23, 59, 59, 999999)),
    ],
)
def test_convert_logical_types_time_boundary_values(
    logical_type: str, base_type: str, value: int, expected: datetime.time
) -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps({"type": base_type, "logicalType": logical_type}))
    assert convert_logical_types(typed_schema.schema, value) == expected


def test_convert_logical_types_date_before_epoch() -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps({"type": "int", "logicalType": "date"}))
    assert convert_logical_types(typed_schema.schema, -1) == datetime.date(1969, 12, 31)
    assert convert_logical_types(typed_schema.schema, -719162) == datetime.date(1, 1, 1)


def test_convert_logical_types_date_out_of_range_raises() -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps({"type": "int", "logicalType": "date"}))
    with pytest.raises(InvalidPayload, match="out of the representable range"):
        convert_logical_types(typed_schema.schema, 2**31 - 1)


def test_convert_logical_types_timestamp_micros_int64_bounds() -> None:
    """int64 extremes must produce a clear error instead of an unhandled OverflowError."""
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO, json.dumps({"type": "long", "logicalType": "timestamp-micros"})
    )

    for value in (2**63 - 1, -(2**63)):
        with pytest.raises(InvalidPayload, match="out of the representable range"):
            convert_logical_types(typed_schema.schema, value)

    # Extreme but representable values still convert.
    max_supported = datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)
    max_micros = (max_supported - _EPOCH) // datetime.timedelta(microseconds=1)
    converted = convert_logical_types(typed_schema.schema, max_micros)
    assert converted == max_supported


@pytest.mark.parametrize(
    "schema_json,value,assertion",
    [
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            "2020-09-13T12:26:40Z",
            lambda v: (
                isinstance(v, datetime.datetime)
                and v == datetime.datetime(2020, 9, 13, 12, 26, 40, tzinfo=datetime.timezone.utc)
            ),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            "2020-09-13T17:26:40+05:00",
            lambda v: (
                isinstance(v, datetime.datetime)
                and v == datetime.datetime(2020, 9, 13, 12, 26, 40, tzinfo=datetime.timezone.utc)
            ),
        ),
        # Example from Avro spec (https://avro.apache.org/docs/1.12.0/specification/#time_ms):
        # noon in Helsinki (UTC+2) is shifted to 10:00 UTC → Avro long 946720800000 ms.
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            "2000-01-01T12:00:00+02:00",
            lambda v: (
                isinstance(v, datetime.datetime)
                and v == datetime.datetime(2000, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
            ),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            "2000-01-01T12:00:00+02:00",
            lambda v: (
                isinstance(v, datetime.datetime)
                and v == datetime.datetime(2000, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
            ),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            "2020-09-13T12:26:40",
            lambda v: (
                isinstance(v, datetime.datetime)
                and v == datetime.datetime(2020, 9, 13, 12, 26, 40, tzinfo=datetime.timezone.utc)
            ),
        ),
        (
            {"type": "int", "logicalType": "date"},
            "2019-04-14",
            lambda v: isinstance(v, datetime.date) and v == datetime.date(2019, 4, 14),
        ),
        (
            {"type": "int", "logicalType": "time-millis"},
            "12:00:00.123",
            lambda v: isinstance(v, datetime.time) and v == datetime.time(12, 0, 0, 123000),
        ),
        (
            {"type": "long", "logicalType": "time-micros"},
            "12:00:00.123456",
            lambda v: isinstance(v, datetime.time) and v == datetime.time(12, 0, 0, 123456),
        ),
    ],
)
def test_convert_logical_types_iso8601_extended_parser(schema_json, value, assertion) -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    converted = convert_logical_types(typed_schema.schema, value, extended_json_parser=True)
    assert assertion(converted)


@pytest.mark.parametrize(
    "schema_json,value",
    [
        ({"type": "long", "logicalType": "timestamp-millis"}, "2020-09-13T12:26:40Z"),
        ({"type": "int", "logicalType": "date"}, "2019-04-14"),
        ({"type": "long", "logicalType": "time-micros"}, "12:00:00.123456"),
    ],
)
def test_convert_logical_types_iso8601_disabled_returns_original(schema_json, value) -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    converted_disabled = convert_logical_types(typed_schema.schema, value, extended_json_parser=False)
    assert converted_disabled == value


@pytest.mark.parametrize(
    "schema_json",
    [
        {"type": "long", "logicalType": "timestamp-millis"},
        {"type": "long", "logicalType": "timestamp-micros"},
        {"type": "int", "logicalType": "date"},
        {"type": "int", "logicalType": "time-millis"},
        {"type": "long", "logicalType": "time-micros"},
    ],
)
def test_convert_logical_types_iso8601_invalid_returns_original(schema_json) -> None:
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema_json))

    converted_invalid = convert_logical_types(typed_schema.schema, "not-an-iso", extended_json_parser=True)
    assert converted_invalid == "not-an-iso"


def test_avro_json_write_invalid(karapace_container: KarapaceContainer) -> None:
    schema = {
        "namespace": "io.aiven.data",
        "name": "Test",
        "type": "record",
        "fields": [
            {
                "name": "attr",
                "type": ["null", "string"],
            }
        ],
    }
    records = [
        {"attr": {"string": 5}},
        {"attr": {"foo": "bar"}},
        {"foo": "bar"},
    ]

    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema))
    bio = io.BytesIO()

    for record in records:
        with pytest.raises(avro.errors.AvroTypeException):
            write_value(karapace_container.config(), typed_schema, bio, record)


def test_avro_json_write_accepts_json_encoded_data_without_tagged_unions(karapace_container: KarapaceContainer) -> None:
    """Backwards compatibility test for Avro data using JSON encoding.

    The initial behavior of the API was incorrect, and it accept data with
    invalid encoding for union types.

    Given this schema:

        {
          "namespace": "io.aiven.data",
          "name": "Test",
          "type": "record",
          "fields": [
            {"name": "attr", "type": ["null", "string"]}
          ]
        }

    The correct JSON encoding for the `attr` field is:

        {"attr":{"string":"sample data"}}

    However, because of the lack of a parser for Avro data JSON-encoded, the
    following was accepted by the server (note the missing tag):

        {"attr":"sample data"}

    This tests the broken behavior is still supported for backwards
    compatibility.
    """

    # Regression test: The same value must be used as the record name and one
    # of the record fields. An initial iteration of write_value would always
    # call flatten_unions, which broker backwards compatibility by corrupting
    # the old format (i.e. the missing_tag_encoding_a value below should be
    # kept unadulterated).
    duplicated_name = "somename"

    schema = {
        "namespace": "io.aiven.data",
        "name": "Test",
        "type": "record",
        "fields": [
            {
                "name": "outter",
                "type": [
                    {"type": "record", "name": duplicated_name, "fields": [{"name": duplicated_name, "type": "string"}]},
                    "int",
                ],
            }
        ],
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema))

    properly_tagged_encoding_a = {"outter": {duplicated_name: {duplicated_name: "data"}}}
    properly_tagged_encoding_b = {"outter": {"int": 1}}
    missing_tag_encoding_a = {"outter": {duplicated_name: "data"}}
    missing_tag_encoding_b = {"outter": 1}

    buffer_a = io.BytesIO()
    buffer_b = io.BytesIO()
    write_value(karapace_container.config(), typed_schema, buffer_a, properly_tagged_encoding_a)
    write_value(karapace_container.config(), typed_schema, buffer_b, missing_tag_encoding_a)
    assert buffer_a.getbuffer() == buffer_b.getbuffer()

    buffer_a = io.BytesIO()
    buffer_b = io.BytesIO()
    write_value(karapace_container.config(), typed_schema, buffer_a, properly_tagged_encoding_b)
    write_value(karapace_container.config(), typed_schema, buffer_b, missing_tag_encoding_b)
    assert buffer_a.getbuffer() == buffer_b.getbuffer()


def test_write_value_strict_mode_rejects_shortname_tag(karapace_container: KarapaceContainer) -> None:
    """In strict mode, namespaced union records must use fullname wrapper keys."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(NAMESPACED_UNION_SCHEMA))
    # Copy the session-scoped config so the override does not leak to other tests.
    config = karapace_container.config().model_copy(update={"rest_avro_permissive_json_parser": False})
    payload = {"payload": {"Payload": {"amount": 2.3}}}

    with pytest.raises(InvalidPayload):
        write_value(config, typed_schema, io.BytesIO(), payload)


def test_write_value_strict_mode_accepts_fullname_tag(karapace_container: KarapaceContainer) -> None:
    """In strict mode, fullname wrapper keys are accepted for namespaced union records."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(NAMESPACED_UNION_SCHEMA))
    # Copy the session-scoped config so the override does not leak to other tests.
    config = karapace_container.config().model_copy(update={"rest_avro_permissive_json_parser": False})
    payload = {"payload": {"org.polyus.ipl.ds.erd.doc.asdfasdf.ver1.Payload": {"amount": 2.3}}}

    write_value(config, typed_schema, io.BytesIO(), payload)


def test_write_value_permissive_mode_still_accepts_shortname_tag(karapace_container: KarapaceContainer) -> None:
    """Permissive mode keeps short-name compatibility for existing clients."""
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(NAMESPACED_UNION_SCHEMA))
    # Copy the session-scoped config so the override does not leak to other tests.
    config = karapace_container.config().model_copy(update={"rest_avro_permissive_json_parser": True})
    payload = {"payload": {"Payload": {"amount": 2.3}}}

    write_value(config, typed_schema, io.BytesIO(), payload)


async def test_serialization_fails(karapace_container: KarapaceContainer):
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject(Subject("topic"))
        await serializer.serialize(schema, {"foo": "bar"})

    assert mock_registry_client.method_calls == [call.get_schema("topic")]


async def test_deserialization_fails(karapace_container: KarapaceContainer):
    mock_registry_client = Mock()
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    deserializer = await make_ser_deser(karapace_container, mock_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    # for now we ignore the packed in schema id
    invalid_data_payload = struct.pack(">bII", START_BYTE, 1, 500)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(invalid_data_payload)

    assert mock_registry_client.method_calls == [call.get_schema_for_id(1)]
    # Reset mock, next test calls the function also.
    mock_registry_client.reset_mock()

    # but we can pass in a perfectly fine doc belonging to a diff schema
    schema, _ = await mock_registry_client.get_schema_for_id(1)
    schema = copy.deepcopy(schema.to_dict())
    schema["name"] = "BadUser"
    schema["fields"][0]["type"] = "int"
    obj = {"name": 100, "favorite_number": 2, "favorite_color": "bar"}
    writer = avro.io.DatumWriter(avro.schema.make_avsc_object(schema))
    with io.BytesIO() as bio:
        enc = avro.io.BinaryEncoder(bio)
        bio.write(struct.pack(HEADER_FORMAT, START_BYTE, 1))
        writer.write(obj, enc)
        enc_bytes = bio.getvalue()
    # Avro 1.11.0 does not assert anymore if the bytes io read function
    # gives back the number of bytes expected. The invalid Avro record
    # read on following manner:
    #  * expected field is name and read as bytes
    #  * read long to indicate how many bytes are in the string = 100
    #  * 100 bytes is read from bytes io, returns 4 (b'\x04\x06bar')
    #  * bytes io position is at the end of the byte buffer
    #  * expected field is favorite number and is read as single int/long
    #  * bytes buffer is at the end and returns zero data
    #  * Avro calls `ord` with zero data and TypeError is raised.
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(enc_bytes)

    assert mock_registry_client.method_calls == [call.get_schema_for_id(1)]


async def test_deserialization_propagates_schema_retrieval_error(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    mock_registry_client.get_schema_for_id.side_effect = SchemaRetrievalError("schema registry unavailable")

    deserializer = await make_ser_deser(karapace_container, mock_registry_client)
    payload = struct.pack(">bI", START_BYTE, 1)

    with pytest.raises(SchemaRetrievalError, match="schema registry unavailable"):
        await deserializer.deserialize(payload)

    assert mock_registry_client.method_calls == [call.get_schema_for_id(1)]


async def test_deserialize_offloads_avro_read_to_thread(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, COMPLEX_UNION_AVRO_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((COMPLEX_UNION_AVRO_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "id": "one",
        "attrs": [
            {"k": "text", "v": "value"},
            {"k": "count", "v": 5},
            {"k": "ratio", "v": 1.5},
            {"k": "flag", "v": True},
            {"k": "empty", "v": None},
        ],
        "props": {"present": "yes", "missing": None},
    }
    payload = await serializer.serialize(schema, record)

    to_thread_calls: list[str] = []

    async def fake_to_thread(func, *args, **kwargs):
        to_thread_calls.append(func.__name__)
        return func(*args, **kwargs)

    with patch("karapace.core.serialization.asyncio.to_thread", side_effect=fake_to_thread):
        assert await serializer.deserialize(payload) == record

    assert to_thread_calls == ["_read_avro_value"]


async def test_deserialize_reuses_datum_reader_for_map_union_schema(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, MAP_UNION_AVRO_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((MAP_UNION_AVRO_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {"id": "one", "props": {"present": "yes", "missing": None}}
    payload = await serializer.serialize(schema, record)
    original_datum_reader = avro.io.DatumReader
    datum_reader_init_count = 0

    class CountingDatumReader:
        def __init__(self, writers_schema):
            nonlocal datum_reader_init_count
            datum_reader_init_count += 1
            self._delegate = original_datum_reader(writers_schema=writers_schema)

        def read(self, decoder):
            return self._delegate.read(decoder)

    with patch("karapace.core.serialization.DatumReader", CountingDatumReader):
        assert await serializer.deserialize(payload) == record
        assert await serializer.deserialize(payload) == record

    assert datum_reader_init_count == 1


async def test_deserialize_reuses_datum_reader_for_complex_avro_schema(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, COMPLEX_UNION_AVRO_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((COMPLEX_UNION_AVRO_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "id": "one",
        "attrs": [
            {"k": "text", "v": "value"},
            {"k": "count", "v": 5},
            {"k": "ratio", "v": 1.5},
            {"k": "flag", "v": True},
        ],
        "props": {"present": "yes", "missing": None},
    }
    payload = await serializer.serialize(schema, record)
    original_datum_reader = avro.io.DatumReader
    datum_reader_init_count = 0

    class CountingDatumReader:
        def __init__(self, writers_schema):
            nonlocal datum_reader_init_count
            datum_reader_init_count += 1
            self._delegate = original_datum_reader(writers_schema=writers_schema)

        def read(self, decoder):
            return self._delegate.read(decoder)

    with patch("karapace.core.serialization.DatumReader", CountingDatumReader):
        assert await serializer.deserialize(payload) == record
        assert await serializer.deserialize(payload) == record

    assert datum_reader_init_count == 1


async def test_deserialize_converts_avro_bytes_to_base64_strings(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, AVRO_BYTES_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((AVRO_BYTES_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "payload": {
            "raw": b"\x01\x02",
            "items": [b"\x03\x04", b"\x05\x06"],
        }
    }
    payload = await serializer.serialize(schema, record)

    assert await serializer.deserialize(payload) == {
        "payload": {
            "raw": base64.b64encode(b"\x01\x02").decode("ascii"),
            "items": [
                base64.b64encode(b"\x03\x04").decode("ascii"),
                base64.b64encode(b"\x05\x06").decode("ascii"),
            ],
        }
    }


async def test_deserialize_converts_empty_avro_bytes_to_empty_base64_strings(
    karapace_container: KarapaceContainer,
) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, AVRO_BYTES_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((AVRO_BYTES_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "payload": {
            "raw": b"",
            "items": [b"", b""],
        }
    }
    payload = await serializer.serialize(schema, record)

    assert await serializer.deserialize(payload) == {
        "payload": {
            "raw": "",
            "items": ["", ""],
        }
    }


async def test_avro_bytes_consume_produce_round_trip(karapace_container: KarapaceContainer) -> None:
    """A record consumed through the REST proxy (bytes as base64) must produce back unchanged."""
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, AVRO_BYTES_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((AVRO_BYTES_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "payload": {
            "raw": b"\x01\x02",
            "items": [b"\x03\x04", b"\x05\x06"],
        }
    }
    payload = await serializer.serialize(schema, record)
    consumed = await serializer.deserialize(payload)
    assert consumed["payload"]["raw"] == "AQI="

    # Producing the consumed JSON must yield the identical binary payload.
    reproduced_payload = await serializer.serialize(schema, consumed)
    assert reproduced_payload == payload
    assert await serializer.deserialize(reproduced_payload) == consumed


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("foo-key"), NameStrategy.topic_name, SubjectType.key),
        (Subject("io.aiven.data.Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-io.aiven.data.Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("foo-value"), NameStrategy.topic_name, SubjectType.value),
        (Subject("io.aiven.data.Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-io.aiven.data.Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_avro(expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType):
    assert (
        get_subject_name(topic_name="foo", schema=TYPED_AVRO_SCHEMA, subject_type=subject_type, naming_strategy=strategy)
        == expected_subject
    )


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_json_schema(expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType):
    assert (
        get_subject_name(topic_name="foo", schema=TYPED_JSON_SCHEMA, subject_type=subject_type, naming_strategy=strategy)
        == expected_subject
    )


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_avro_without_namespace(
    expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType
):
    assert (
        get_subject_name(
            topic_name="foo", schema=TYPED_AVRO_SCHEMA_WITHOUT_NAMESPACE, subject_type=subject_type, naming_strategy=strategy
        )
        == expected_subject
    )


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_protobuf(expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType):
    assert (
        get_subject_name(topic_name="foo", schema=TYPED_PROTOBUF_SCHEMA, subject_type=subject_type, naming_strategy=strategy)
        == expected_subject
    )


# Authorization forwarding via sr_authorization_ctx. Tested through observed headers
# on the mocked Client — covers post_new_schema, _get_schema_recursive, get_schema_for_id,
# and the @alru_cache partitioning on get_schema.


def _make_result(json_result: dict, status: int = 200) -> Mock:
    result = Mock()
    result.ok = 200 <= status < 300
    result.status_code = status
    result.json = Mock(return_value=json_result)
    return result


async def test_post_new_schema_forwards_authorization_header(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    post_future = asyncio.Future()
    post_future.set_result(_make_result({"id": 42}))
    sr_client.client.post = Mock(return_value=post_future)

    sr_authorization_ctx.set("Bearer fwd.token")
    schema = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    schema_id = await sr_client.post_new_schema("subj", schema)

    assert schema_id == 42
    _, kwargs = sr_client.client.post.call_args
    # Authorization is forwarded; SR vendor Content-Type is preserved.
    assert kwargs["headers"] == {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
        "Authorization": "Bearer fwd.token",
    }


async def test_post_new_schema_no_authorization_header_when_ctx_unset(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    post_future = asyncio.Future()
    post_future.set_result(_make_result({"id": 42}))
    sr_client.client.post = Mock(return_value=post_future)

    schema = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    await sr_client.post_new_schema("subj", schema)

    _, kwargs = sr_client.client.post.call_args
    # Ctx unset → no Authorization; vendor Content-Type stays.
    assert kwargs["headers"] == {"Content-Type": "application/vnd.schemaregistry.v1+json"}


async def test_post_new_schema_treats_empty_token_as_unset(reset_sr_authorization_ctx) -> None:
    """Empty contextvar string must not produce an `Authorization: ` header."""
    sr_client = SchemaRegistryClient()
    post_future = asyncio.Future()
    post_future.set_result(_make_result({"id": 42}))
    sr_client.client.post = Mock(return_value=post_future)

    sr_authorization_ctx.set("")
    schema = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    await sr_client.post_new_schema("subj", schema)

    _, kwargs = sr_client.client.post.call_args
    assert "Authorization" not in kwargs["headers"]
    assert kwargs["headers"] == {"Content-Type": "application/vnd.schemaregistry.v1+json"}


async def test_get_schema_for_id_forwards_authorization_header(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    get_future = asyncio.Future()
    get_future.set_result(
        _make_result(
            {
                "schema": schema_avro_json,
                "subjects": ["subj"],
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )
    sr_client.client.get = Mock(return_value=get_future)

    sr_authorization_ctx.set("Bearer xyz")
    await sr_client.get_schema_for_id(1)

    _, kwargs = sr_client.client.get.call_args
    assert kwargs["headers"] == {"Authorization": "Bearer xyz"}


async def test_get_schema_recursive_forwards_authorization_header(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    get_future = asyncio.Future()
    get_future.set_result(
        _make_result(
            {
                "id": 7,
                "schema": schema_avro_json,
                "version": 1,
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )
    sr_client.client.get = Mock(return_value=get_future)

    sr_authorization_ctx.set("Bearer recursive")
    # Bypass @alru_cache on get_schema.
    schema_id, _, _ = await sr_client._get_schema_recursive(Subject("subj"), set(), None)

    assert schema_id == 7
    _, kwargs = sr_client.client.get.call_args
    assert kwargs["headers"] == {"Authorization": "Bearer recursive"}


async def test_get_schema_cache_partitions_by_token(reset_sr_authorization_ctx) -> None:
    """Cache key includes the token fingerprint: same token hits cache, different token misses."""

    sr_client = SchemaRegistryClient()
    sr_client.client.get = AsyncMock(
        return_value=_make_result(
            {
                "id": 11,
                "schema": schema_avro_json,
                "version": 1,
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )

    subject = Subject("uniq-subject-for-cache-partition-test")

    sr_authorization_ctx.set("Bearer first")
    await sr_client.get_schema(subject)
    await sr_client.get_schema(subject)  # same token — cache hit
    assert sr_client.client.get.call_count == 1

    sr_authorization_ctx.set("Bearer second")
    await sr_client.get_schema(subject)  # different token — cache miss, SR is consulted again
    assert sr_client.client.get.call_count == 2

    sr_authorization_ctx.set("Bearer first")
    await sr_client.get_schema(subject)  # back to first token — cache hit
    assert sr_client.client.get.call_count == 2


async def test_get_schema_cache_unauthenticated_path_unchanged(reset_sr_authorization_ctx) -> None:
    """Unauthenticated path: empty fingerprint, back-to-back calls still hit cache."""
    sr_client = SchemaRegistryClient()
    get_future = asyncio.Future()
    get_future.set_result(
        _make_result(
            {
                "id": 12,
                "schema": schema_avro_json,
                "version": 1,
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )
    sr_client.client.get = Mock(return_value=get_future)

    subject = Subject("uniq-subject-for-cache-unauth-test")
    await sr_client.get_schema(subject)
    await sr_client.get_schema(subject)
    assert sr_client.client.get.call_count == 1
