"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiohttp import BasicAuth
from async_lru import alru_cache
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from cachetools import TTLCache
from collections.abc import Callable, MutableMapping
from google.protobuf.message import DecodeError
from jsonschema import ValidationError
from karapace.core.client import Client
from karapace.core.config import Config
from karapace.core.dependency import Dependency
from karapace.core.errors import InvalidReferences
from karapace.core.protobuf.exception import ProtobufTypeException
from karapace.core.protobuf.io import ProtobufDatumReader, ProtobufDatumWriter
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.schema_models import (
    InvalidSchema,
    ParsedTypedSchema,
    SchemaType,
    TypedSchema,
    ValidatedTypedSchema,
    Versioner,
)
from karapace.core.schema_references import LatestVersionReference, Reference, reference_from_mapping
from karapace.core.typing import NameStrategy, SchemaId, Subject, SubjectType, Version
from karapace.core.utils import json_decode, json_encode
from typing import Any
from urllib.parse import quote

import asyncio
import avro
import avro.schema
import base64
import contextvars
import datetime
import decimal
import hashlib
import io
import re
import struct
import threading
import weakref

# Per-request Authorization header forwarded from the REST Proxy to SR; None falls back to
# session_auth. Only set in UserRestProxy.publish/fetch — other proxy endpoints don't reach
# the serializer. Keep this list current if that changes.
sr_authorization_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar("sr_authorization", default=None)


def _authorization_headers() -> dict[str, str] | None:
    auth = sr_authorization_ctx.get()
    return {"Authorization": auth} if auth else None


def _token_fingerprint() -> str:
    """Stable cache-key fingerprint for the current Authorization. Empty string when unset."""
    auth = sr_authorization_ctx.get()
    if not auth:
        return ""
    # SHA-256 truncated to 16 hex chars; never logged, never stores raw bearers in the LRU.
    return hashlib.sha256(auth.encode("utf-8")).hexdigest()[:16]


START_BYTE = 0x0
HEADER_FORMAT = ">bI"
HEADER_SIZE = 5

_EPOCH_DATETIME = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_DATE = datetime.date(1970, 1, 1)
_MILLIS_PER_DAY = 86_400_000
_MICROS_PER_DAY = 86_400_000_000
_DECIMAL_TEN = decimal.Decimal(10)
_DECIMAL_STRING_RE = re.compile(r"-?\d+(\.\d+)?([Ee][+-]?\d+)?")
# Note: local-timestamp-millis/micros are intentionally absent. The patched avro
# library does not implement them (avro.schema.parse warns and produces a plain
# "long" with logical_type=None), so they can never reach logical-type handling.
_LOGICAL_TYPE_FORMAT_HINTS: dict[str, str] = {
    "date": 'ISO-8601 date (e.g. "2025-05-05")',
    "timestamp-millis": 'ISO-8601 datetime with timezone (e.g. "2025-05-05T16:29:00.123+04:00")',
    "timestamp-micros": 'ISO-8601 datetime with timezone (e.g. "2025-05-05T16:29:00.123456+04:00")',
    "time-millis": 'ISO-8601 time (e.g. "16:29:00.123")',
    "time-micros": 'ISO-8601 time (e.g. "16:29:00.123456")',
    "decimal": 'numeric string (e.g. "14.36")',
}


def _decimal_fits_precision(value: decimal.Decimal, precision: int, scale: int) -> bool:
    """Validate Avro decimal precision against schema precision/scale."""
    if precision <= 0:
        return True
    quantized = value.quantize(_DECIMAL_TEN**-scale, rounding=decimal.ROUND_HALF_UP)
    unscaled = int(quantized.scaleb(scale))
    digits = len(str(abs(unscaled)))
    return digits <= precision


def _decimal_from_number(value: int | str, precision: int, scale: int) -> decimal.Decimal:
    """Convert a JSON int or numeric string to a Decimal honouring schema scale/precision.

    More fractional digits than the schema scale is an error: silently rounding
    would corrupt data the client believes was stored exactly.
    """
    try:
        parsed = decimal.Decimal(str(value))
    except decimal.InvalidOperation as e:
        raise InvalidPayload(f"{value!r} is not a valid decimal value") from e
    with decimal.localcontext() as ctx:
        ctx.prec = max(ctx.prec, len(parsed.as_tuple().digits) + scale + 4)
        converted = parsed.quantize(_DECIMAL_TEN**-scale)
        if converted != parsed:
            raise InvalidPayload(f"{value!r} has more fractional digits than the schema scale ({scale}) allows")
    if not _decimal_fits_precision(converted, precision=precision, scale=scale):
        raise InvalidPayload(f"{value!r} does not fit schema decimal precision {precision} with scale {scale}")
    return converted


class DeserializationError(Exception):
    pass


class InvalidMessageHeader(Exception):
    pass


class InvalidPayload(Exception):
    pass


class InvalidMessageSchema(Exception):
    pass


class SchemaError(Exception):
    pass


class SchemaRetrievalError(SchemaError):
    pass


class SchemaUpdateError(SchemaError):
    pass


class InvalidRecord(Exception):
    pass


def topic_name_strategy(
    topic_name: str,
    record_name: str | None,
    subject_type: SubjectType,
) -> Subject:
    return Subject(f"{topic_name}-{subject_type}")


def record_name_strategy(
    topic_name: str,
    record_name: str | None,
    subject_type: SubjectType,
) -> Subject:
    if record_name is None:
        raise InvalidRecord(
            "The provided record doesn't have a valid `record_name`, use another naming strategy or fix the schema"
        )

    return Subject(record_name)


def topic_record_name_strategy(
    topic_name: str,
    record_name: str | None,
    subject_type: SubjectType,
) -> Subject:
    validated_record_name = record_name_strategy(topic_name, record_name, subject_type)
    return Subject(f"{topic_name}-{validated_record_name}")


NAME_STRATEGIES = {
    NameStrategy.topic_name: topic_name_strategy,
    NameStrategy.record_name: record_name_strategy,
    NameStrategy.topic_record_name: topic_record_name_strategy,
}


class SchemaRegistryClient:
    def __init__(
        self,
        schema_registry_url: str = "http://localhost:8081",
        server_ca: str | None = None,
        session_auth: BasicAuth | None = None,
        *,
        cache_maxsize: int = Config.model_fields["schema_registry_client_cache_maxsize"].default,
    ):
        self.client = Client(server_uri=schema_registry_url, server_ca=server_ca, session_auth=session_auth)
        self.base_url = schema_registry_url
        # Per-instance decoration so cache_maxsize can come from Config.
        self._get_schema_cached = alru_cache(maxsize=cache_maxsize)(self._get_schema_cached)

    async def post_new_schema(
        self, subject: str, schema: ValidatedTypedSchema, references: Reference | None = None
    ) -> SchemaId:
        if schema.schema_type is SchemaType.PROTOBUF:
            if references:
                payload = {"schema": str(schema), "schemaType": schema.schema_type.value, "references": references.json()}
            else:
                payload = {"schema": str(schema), "schemaType": schema.schema_type.value}
        else:
            payload = {"schema": json_encode(schema.to_dict()), "schemaType": schema.schema_type.value}
        # Client.post only injects the vendor Content-Type when headers is falsy; merge so
        # forwarding Authorization doesn't silently demote the request to application/json.
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json", **(_authorization_headers() or {})}
        result = await self.client.post(f"subjects/{quote(subject)}/versions", json=payload, headers=headers)
        if not result.ok:
            raise SchemaRetrievalError(result.json())
        return SchemaId(result.json()["id"])

    async def _get_schema_recursive(
        self,
        subject: Subject,
        explored_schemas: set[tuple[Subject, Version | None]],
        version: Version | None = None,
    ) -> tuple[SchemaId, ValidatedTypedSchema, Version]:
        if (subject, version) in explored_schemas:
            raise InvalidSchema(
                f"The schema has at least a cycle in dependencies, "
                f"one path of the cycle is given by the following nodes: {explored_schemas}"
            )

        explored_schemas = explored_schemas | {(subject, version)}

        version_str = str(version) if version is not None else "latest"
        result = await self.client.get(f"subjects/{quote(subject)}/versions/{version_str}", headers=_authorization_headers())

        if not result.ok:
            raise SchemaRetrievalError(result.json())

        json_result = result.json()
        if "id" not in json_result or "schema" not in json_result or "version" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")

        if "references" in json_result:
            references = [Reference.from_dict(data) for data in json_result["references"]]
            dependencies = {}
            for reference in references:
                _, schema, version = await self._get_schema_recursive(reference.subject, explored_schemas, reference.version)
                dependencies[reference.name] = Dependency(
                    name=reference.name, subject=reference.subject, version=version, target_schema=schema
                )
        else:
            references = None
            dependencies = None

        try:
            schema_type = SchemaType(json_result.get("schemaType", "AVRO"))
            return (
                SchemaId(json_result["id"]),
                ValidatedTypedSchema.parse(
                    schema_type,
                    json_result["schema"],
                    references=references,
                    dependencies=dependencies,
                ),
                Versioner.V(json_result["version"]),
            )
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def get_schema(
        self,
        subject: Subject,
        version: Version | None = None,
    ) -> tuple[SchemaId, ValidatedTypedSchema, Version]:
        """
        Retrieves the schema and its dependencies for the specified subject.

        Args:
            subject (Subject): The subject for which to retrieve the schema.
            version (Optional[Version]): The specific version of the schema to retrieve.
                                                    If None, the latest available schema will be returned.

        Returns:
            Tuple[SchemaId, ValidatedTypedSchema, Version]: A tuple containing:
                - SchemaId: The ID of the retrieved schema.
                - ValidatedTypedSchema: The retrieved schema, validated and typed.
                - Version: The version of the schema that was retrieved.
        """
        # Partition the cache by token fingerprint: each principal is validated by SR at
        # least once per fingerprint per cache lifetime, not per request. Cached entries
        # outlive server-side token expiry — acceptable for immutable schema bytes, not a
        # substitute for revocation. Empty fingerprint = the unauthenticated slot.
        return await self._get_schema_cached(subject, version, _token_fingerprint())

    async def _get_schema_cached(
        self,
        subject: Subject,
        version: Version | None,
        token_fingerprint: str,  # cache-key only; partitions the LRU per principal
    ) -> tuple[SchemaId, ValidatedTypedSchema, Version]:
        return await self._get_schema_recursive(subject, set(), version)

    async def get_schema_for_id(self, schema_id: SchemaId) -> tuple[TypedSchema, list[Subject]]:
        result = await self.client.get(
            f"schemas/ids/{schema_id}", params={"includeSubjects": "True"}, headers=_authorization_headers()
        )
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "schema" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")

        subjects = json_result.get("subjects")

        try:
            schema_type = SchemaType(json_result.get("schemaType", "AVRO"))

            references = json_result.get("references")
            parsed_references = None
            if references:
                parsed_references = []
                for reference_data in references:
                    try:
                        reference = reference_from_mapping(reference_data)
                    except (TypeError, KeyError) as exc:
                        raise InvalidReferences from exc
                    parsed_references.append(reference)
            if parsed_references:
                dependencies = {}

                for reference in parsed_references:
                    if isinstance(reference, LatestVersionReference):
                        _, schema, version = await self.get_schema(reference.subject)
                    else:
                        _, schema, version = await self.get_schema(reference.subject, reference.version)

                    dependencies[reference.name] = Dependency(reference.name, reference.subject, version, schema)
            else:
                dependencies = None

            return (
                ParsedTypedSchema.parse(
                    schema_type, json_result["schema"], references=parsed_references, dependencies=dependencies
                ),
                subjects,
            )
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def close(self):
        await self.client.close()


def get_subject_name(
    topic_name: str,
    schema: TypedSchema,
    subject_type: SubjectType,
    naming_strategy: NameStrategy,
) -> Subject:
    record_name = None

    if schema.schema_type is SchemaType.AVRO:
        if isinstance(schema.schema, avro.schema.NamedSchema):
            record_name = schema.schema.fullname
        else:
            record_name = None

    if schema.schema_type is SchemaType.JSONSCHEMA:
        record_name = schema.to_dict().get("title", None)

    if schema.schema_type is SchemaType.PROTOBUF:
        assert isinstance(schema.schema, ProtobufSchema), "Expecting a protobuf schema"
        record_name = schema.schema.record_name()

    naming_strategy = NAME_STRATEGIES[naming_strategy]
    return naming_strategy(topic_name, record_name, subject_type)


class SchemaRegistrySerializer:
    def __init__(
        self,
        config: Config,
    ) -> None:
        self.config = config
        self.state_lock = asyncio.Lock()
        registry_url = f"{self.config.registry_scheme}://{self.config.registry_host}:{self.config.registry_port}"
        session_auth: BasicAuth | None = None
        if self.config.registry_user and self.config.registry_password:
            session_auth = BasicAuth(self.config.registry_user, self.config.registry_password, encoding="utf8")
        cache_maxsize = self.config.schema_registry_client_cache_maxsize
        if self.config.registry_ca:
            registry_client = SchemaRegistryClient(
                registry_url,
                server_ca=self.config.registry_ca,
                session_auth=session_auth,
                cache_maxsize=cache_maxsize,
            )
        else:
            registry_client = SchemaRegistryClient(registry_url, session_auth=session_auth, cache_maxsize=cache_maxsize)
        self.registry_client: SchemaRegistryClient | None = registry_client
        self.ids_to_schemas: dict[int, TypedSchema] = {}
        self.ids_to_subjects: MutableMapping[int, list[Subject]] = TTLCache(maxsize=10000, ttl=600)
        self.schemas_to_ids: dict[str, SchemaId] = {}
        self._avro_readers_lock = threading.Lock()
        self._avro_readers_by_thread: weakref.WeakKeyDictionary[threading.Thread, TTLCache[SchemaId, DatumReader]] = (
            weakref.WeakKeyDictionary()
        )

    async def close(self) -> None:
        if self.registry_client:
            await self.registry_client.close()
            self.registry_client = None

    async def get_schema_for_subject(self, subject: Subject) -> TypedSchema:
        assert self.registry_client, "must not call this method after the object is closed."
        schema_id, schema, _ = await self.registry_client.get_schema(subject)
        async with self.state_lock:
            schema_ser = str(schema)
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema
        return schema

    async def upsert_id_for_schema(self, schema_typed: ValidatedTypedSchema, subject: str) -> SchemaId:
        assert self.registry_client, "must not call this method after the object is closed."

        schema_ser = str(schema_typed)

        if schema_ser in self.schemas_to_ids:
            return self.schemas_to_ids[schema_ser]

        # note: the post is idempotent, so it is like a get or insert (aka upsert)
        schema_id = await self.registry_client.post_new_schema(subject, schema_typed)

        async with self.state_lock:
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema_typed
        return schema_id

    async def get_schema_for_id(
        self,
        schema_id: SchemaId,
        *,
        need_new_call: Callable[[TypedSchema, list[Subject]], bool] | None = None,
    ) -> tuple[TypedSchema, list[Subject]]:
        assert self.registry_client, "must not call this method after the object is closed."
        if schema_id in self.ids_to_subjects:
            if need_new_call is None or not need_new_call(self.ids_to_schemas[schema_id], self.ids_to_subjects[schema_id]):
                return self.ids_to_schemas[schema_id], self.ids_to_subjects[schema_id]

        schema_typed, subjects = await self.registry_client.get_schema_for_id(schema_id)
        schema_ser = str(schema_typed)
        async with self.state_lock:
            # todo: get rid of the schema caching and use the same caching used in UserRestProxy
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema_typed
            self.ids_to_subjects[schema_id] = subjects
        return schema_typed, subjects

    async def serialize(self, schema: TypedSchema, value: dict) -> bytes:
        schema_id = self.schemas_to_ids[str(schema)]
        with io.BytesIO() as bio:
            bio.write(struct.pack(HEADER_FORMAT, START_BYTE, schema_id))
            try:
                write_value(self.config, schema, bio, value)
                return bio.getvalue()
            except ProtobufTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e
            except avro.errors.AvroTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e

    async def deserialize(self, bytes_: bytes) -> dict:
        with io.BytesIO(bytes_) as bio:
            byte_arr = bio.read(HEADER_SIZE)
            # we should probably check for compatibility here
            start_byte, schema_id = struct.unpack(HEADER_FORMAT, byte_arr)
            if start_byte != START_BYTE:
                raise InvalidMessageHeader(f"Start byte is {start_byte:x} and should be {START_BYTE:x}")
            try:
                schema, _ = await self.get_schema_for_id(schema_id)
                if schema is None:
                    raise InvalidPayload("No schema with ID from payload")
                if schema.schema_type is SchemaType.AVRO:
                    ret_val = await asyncio.to_thread(self._read_avro_value, SchemaId(schema_id), schema, bio)
                else:
                    ret_val = read_value(self.config, schema, bio)
                return ret_val
            except (UnicodeDecodeError, TypeError, avro.errors.InvalidAvroBinaryEncoding) as e:
                raise InvalidPayload("Data does not contain a valid message") from e
            except avro.errors.SchemaResolutionException as e:
                raise InvalidPayload("Data cannot be decoded with provided schema") from e

    def _get_avro_reader(self, schema_id: SchemaId, schema: TypedSchema) -> DatumReader:
        current_thread = threading.current_thread()
        with self._avro_readers_lock:
            reader_cache = self._avro_readers_by_thread.get(current_thread)
            if reader_cache is None:
                reader_cache = TTLCache(maxsize=10000, ttl=600)
                self._avro_readers_by_thread[current_thread] = reader_cache

        reader = reader_cache.get(schema_id)
        if reader is None:
            reader = DatumReader(writers_schema=schema.schema)
            reader_cache[schema_id] = reader
        return reader

    def _read_avro_value(self, schema_id: SchemaId, schema: TypedSchema, bio: io.BytesIO) -> Any:
        return read_value(self.config, schema, bio, avro_reader=self._get_avro_reader(schema_id, schema))


def _jsonify_avro_payload(value: Any) -> Any:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")

    if isinstance(value, bytearray):
        return base64.b64encode(bytes(value)).decode("ascii")

    if isinstance(value, list):
        return [_jsonify_avro_payload(item) for item in value]

    if isinstance(value, dict):
        return {key: _jsonify_avro_payload(item) for key, item in value.items()}

    return value


def flatten_unions(schema: avro.schema.Schema, value: Any) -> Any:
    """Recursively flattens unions to convert Avro JSON payloads to internal dictionaries

    Data encoded to Avro JSON has a special case for union types, values of these type are encoded
    as tagged union. The additional tag is not expected to be in the internal data format and has to
    be removed before further processing. This means the JSON document must be further processed to
    remove the tag, this function does just that, recursing over the JSON document and handling the
    tagged unions.

    Given this schema:

        {"name": "Test", "type": "record", "fields": [{"name": "attr", "type": ["null", "string"]}]}

    The record JSON encoded as:

        {"attr":{"string":"sample data"}}

    The python representation is:

        {"attr":"sample data"}

    This function:

    - Translates the first to the second when necessary, this adds compatibility for libraries that
      perform the _correct_ encoding.
    - Does nothing if the provided data is already in the second format. The data is improperly
      encoded, but this maintains backwards compatibility.

    See also https://avro.apache.org/docs/current/spec.html#json_encoding
    """

    if isinstance(schema, avro.schema.RecordSchema) and isinstance(value, dict):
        result = dict(value)
        for field in schema.fields:
            if field.name in value:
                result[field.name] = flatten_unions(field.type, value[field.name])
        return result

    if isinstance(schema, avro.schema.UnionSchema) and isinstance(value, dict):

        def get_name(obj) -> str:
            if isinstance(obj, avro.schema.PrimitiveSchema):
                return obj.fullname
            if isinstance(obj, (avro.schema.ArraySchema, avro.schema.MapSchema)):
                return obj.type
            return obj.name

        f = next((s for s in schema.schemas if get_name(s) in value), None)
        if f is not None:
            # Note: This is intentionally skipping the dictionary, here the JSON representation
            # is flattened to the Python representation
            return flatten_unions(f, value[get_name(f)])

    if isinstance(schema, avro.schema.ArraySchema) and isinstance(value, list):
        return [flatten_unions(schema.items, v) for v in value]

    if isinstance(schema, avro.schema.MapSchema) and isinstance(value, dict):
        return {k: flatten_unions(schema.values, v) for (k, v) in value.items()}

    return value


def _unfold_avro_json(
    schema: avro.schema.Schema,
    value: Any,
    extended_json_parser: bool = False,
    *,
    path: str = "value",
) -> Any:
    """Recursively unfold Avro JSON union wrappers using strict branch names.

    Strict mode requires union wrappers to use the exact Avro branch name:
    - named types (record/fixed/enum): fullname
    - primitives: primitive name
    - array/map: type name ("array"/"map")
    """
    if isinstance(schema, avro.schema.RecordSchema) and isinstance(value, dict):
        result = dict(value)
        for field in schema.fields:
            if field.name in value:
                result[field.name] = _unfold_avro_json(
                    field.type,
                    value[field.name],
                    extended_json_parser,
                    path=f"{path}.{field.name}",
                )
        return result

    if isinstance(schema, avro.schema.UnionSchema):
        # In strict mode, union values must be explicitly tagged unless they are
        # the null branch represented by JSON null.
        if value is None:
            has_null_branch = any(
                isinstance(branch, avro.schema.PrimitiveSchema) and branch.fullname == "null" for branch in schema.schemas
            )
            if has_null_branch:
                return value
            raise InvalidPayload(f"{path}: null is not allowed (union does not contain null branch)")

        def get_names(obj: avro.schema.Schema) -> set[str]:
            names: set[str] = set()
            if isinstance(obj, avro.schema.PrimitiveSchema):
                names.add(obj.fullname)
                logical_type = getattr(obj, "logical_type", None)
                if isinstance(logical_type, str):
                    # String-backed logical types (e.g. uuid) are safe in strict mode
                    # without extended parser conversion. Other logical-type tags are
                    # accepted only when extended parser is enabled.
                    if obj.fullname == "string" or extended_json_parser:
                        names.add(logical_type)
                return names
            if isinstance(obj, (avro.schema.ArraySchema, avro.schema.MapSchema)):
                names.add(obj.type)
                return names
            # Use fullname for named types; if there is no namespace this equals short name.
            names.add(obj.fullname)
            return names

        if not isinstance(value, dict) or len(value) != 1:
            allowed_tags = sorted({name for branch in schema.schemas for name in get_names(branch)})
            raise InvalidPayload(
                f'{path}: expected Avro union wrapper object with single key like {{"<tag>": ...}};'
                f" valid tags: {allowed_tags!r}"
            )

        ((tag, wrapped_value),) = value.items()

        matching_branches = [branch for branch in schema.schemas if tag in get_names(branch)]
        if len(matching_branches) != 1:
            allowed_tags = sorted({name for branch in schema.schemas for name in get_names(branch)})
            raise InvalidPayload(
                f"{path}: invalid union tag {tag!r}; expected exactly one of {allowed_tags!r} (got {len(matching_branches)} matches)"
            )

        # Strict path removes the tagged wrapper only when a single branch can
        # be selected from the explicit union tag.
        selected_branch = matching_branches[0]
        is_logical_type_tag = (
            isinstance(selected_branch, avro.schema.PrimitiveSchema)
            and tag != selected_branch.fullname
            and tag == getattr(selected_branch, "logical_type", None)
        )
        if is_logical_type_tag and not isinstance(wrapped_value, str):
            raise InvalidPayload(
                f"{path}: logical type tag {tag!r} only accepts string values; "
                f"use base type tag {selected_branch.fullname!r} for non-string values"
            )
        unfolded_branch_value = _unfold_avro_json(
            selected_branch,
            wrapped_value,
            extended_json_parser,
            path=f"{path}<{tag}>",
        )
        try:
            converted_branch_value = convert_logical_types(selected_branch, unfolded_branch_value, extended_json_parser)
        except InvalidPayload as e:
            raise InvalidPayload(f"{path}: {e}") from e
        # Enforce strict constraints that may be too permissive in generic validate().
        if isinstance(selected_branch, avro.schema.EnumSchema):
            if not isinstance(converted_branch_value, str) or converted_branch_value not in selected_branch.symbols:
                raise InvalidPayload(f"{path}: invalid enum value for union branch {tag!r}")
        if isinstance(selected_branch, avro.schema.FixedSchema):
            if (
                not isinstance(converted_branch_value, (bytes, bytearray))
                or len(converted_branch_value) != selected_branch.size
            ):
                raise InvalidPayload(f"{path}: invalid fixed value for union branch {tag!r}")
        if not avro.io.validate(selected_branch, converted_branch_value):
            if is_logical_type_tag:
                logical_type = getattr(selected_branch, "logical_type", None)
                hint = _LOGICAL_TYPE_FORMAT_HINTS.get(logical_type or "", "")
                hint_part = f"; expected {hint}" if hint else ""
                raise InvalidPayload(f"{path}: {wrapped_value!r} is not a valid {tag!r} string{hint_part}")
            raise InvalidPayload(f"{path}: value does not validate against union branch {tag!r}")
        return converted_branch_value

    if isinstance(schema, avro.schema.ArraySchema) and isinstance(value, list):
        return [_unfold_avro_json(schema.items, v, extended_json_parser, path=f"{path}[{i}]") for i, v in enumerate(value)]

    if isinstance(schema, avro.schema.MapSchema) and isinstance(value, dict):
        return {
            k: _unfold_avro_json(schema.values, v, extended_json_parser, path=f"{path}[{k!r}]") for (k, v) in value.items()
        }

    return value


def convert_logical_types(schema: avro.schema.Schema, value: Any, extended_json_parser: bool = False) -> Any:
    """Recursively coerce JSON-friendly Avro values to logical Python types.
    https://avro.apache.org/docs/++version++/specification/#logical-types

    The function traverses records, arrays, maps, and unions, converting values
    for known logical types:

    - timestamp-millis / timestamp-micros:
        int (ms/µs since epoch) -> timezone-aware UTC datetime.datetime.
        str ISO 8601 (extended_json_parser only) -> UTC datetime.datetime;
        timezone-aware strings are shifted to UTC, naive strings are assumed UTC.
    - date:
        int (days since epoch) -> datetime.date.
        str ISO 8601 (extended_json_parser only) -> datetime.date.
    - time-millis / time-micros:
        int (ms/µs of day) -> datetime.time.
        str ISO 8601 (extended_json_parser only) -> datetime.time.
    - decimal (both parser modes):
        int or numeric string ("123.45", "-7") -> decimal.Decimal; more fractional
          digits than the schema scale raise InvalidPayload (no silent rounding).
        non-numeric string -> Confluent-compatible Base64-encoded two's complement
          unscaled bytes (e.g. "BZw=" for 14.36 at scale=2) -> decimal.Decimal.
        Strings that are neither numeric nor valid base64 raise InvalidPayload.
        float inputs are intentionally not accepted to avoid silent precision loss.

    Args:
        schema: The Avro schema for the current node.
        value: The JSON-decoded value to coerce.
        extended_json_parser: When True, temporal fields additionally accept ISO 8601
            strings. Defaults to False (Confluent-compatible behaviour).

    For unions, each branch is tried in order; the first branch that validates after
    conversion is returned (branches whose conversion raises InvalidPayload are
    skipped). If conversion is not applicable or fails, the original value is
    returned unchanged.
    """
    if isinstance(schema, avro.schema.RecordSchema) and isinstance(value, dict):
        result: dict[Any, Any] = dict(value)
        for field in schema.fields:
            if field.name in value:
                result[field.name] = convert_logical_types(field.type, value[field.name], extended_json_parser)
        return result

    if isinstance(schema, avro.schema.UnionSchema):
        # Try to find a branch schema that validates after conversion.
        for branch in schema.schemas:
            try:
                converted = convert_logical_types(branch, value, extended_json_parser)
            except InvalidPayload:
                # Conversion failed for this branch only: the value may still match
                # another branch (e.g. a plain string next to a logical decimal).
                continue
            if avro.io.validate(branch, converted):
                return converted
        return value

    if isinstance(schema, avro.schema.ArraySchema) and isinstance(value, list):
        return [convert_logical_types(schema.items, v, extended_json_parser) for v in value]

    if isinstance(schema, avro.schema.MapSchema) and isinstance(value, dict):
        return {k: convert_logical_types(schema.values, v, extended_json_parser) for (k, v) in value.items()}

    # Avro JSON encodes bytes/fixed as JSON strings (code points 0-255 map to unsigned bytes).
    # Convert such strings to raw bytes before validation for non-logical bytes/fixed.
    # Logical bytes (e.g. decimal) must continue through logical-type conversion below.
    if (
        isinstance(schema, avro.schema.PrimitiveSchema)
        and not isinstance(schema, avro.schema.LogicalSchema)
        and schema.fullname == "bytes"
        and isinstance(value, str)
    ):
        try:
            return value.encode("latin-1")
        except UnicodeEncodeError:
            return value

    if isinstance(schema, avro.schema.FixedSchema) and isinstance(value, str):
        try:
            return value.encode("latin-1")
        except UnicodeEncodeError:
            return value

    if isinstance(schema, avro.schema.LogicalSchema):
        logical_type = getattr(schema, "logical_type", None)

        # Timestamps
        if logical_type == "timestamp-millis":
            if isinstance(value, int):
                return _EPOCH_DATETIME + datetime.timedelta(milliseconds=value)
            if extended_json_parser and isinstance(value, str):
                try:
                    parsed = datetime.datetime.fromisoformat(value)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
                    return parsed.astimezone(datetime.timezone.utc)
                except ValueError:
                    return value

        if logical_type == "timestamp-micros":
            if isinstance(value, int):
                return _EPOCH_DATETIME + datetime.timedelta(microseconds=value)
            if extended_json_parser and isinstance(value, str):
                try:
                    parsed = datetime.datetime.fromisoformat(value)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
                    return parsed.astimezone(datetime.timezone.utc)
                except ValueError:
                    return value

        # Date
        if logical_type == "date":
            if isinstance(value, int):
                return _EPOCH_DATE + datetime.timedelta(days=value)
            if extended_json_parser and isinstance(value, str):
                try:
                    return datetime.date.fromisoformat(value)
                except ValueError:
                    return value

        # Time
        if logical_type == "time-millis":
            if isinstance(value, int):
                value = value % _MILLIS_PER_DAY
                seconds, millis = divmod(value, 1000)
                hours, rem = divmod(seconds, 3600)
                minutes, seconds = divmod(rem, 60)
                return datetime.time(hour=hours, minute=minutes, second=seconds, microsecond=millis * 1000)
            if extended_json_parser and isinstance(value, str):
                try:
                    return datetime.time.fromisoformat(value)
                except ValueError:
                    return value

        if logical_type == "time-micros":
            if isinstance(value, int):
                value = value % _MICROS_PER_DAY
                seconds, micros = divmod(value, 1_000_000)
                hours, rem = divmod(seconds, 3600)
                minutes, seconds = divmod(rem, 60)
                return datetime.time(hour=hours, minute=minutes, second=seconds, microsecond=micros)
            if extended_json_parser and isinstance(value, str):
                try:
                    return datetime.time.fromisoformat(value)
                except ValueError:
                    return value

        # Decimal: accept numeric values (int or numeric string) or Confluent-style
        # base64-encoded two's complement unscaled bytes (e.g. "BZw=" for 14.36 scale=2).
        if logical_type == "decimal" and isinstance(value, (int, str)):
            scale: int = getattr(schema, "scale", 0)
            precision: int = getattr(schema, "precision", 0)
            # Numeric path first, in both parser modes: ints are unambiguous, and
            # numeric strings must round-trip — a value consumed as "14.36" must
            # produce the same number, and "1436" must mean the number 1436 even
            # though it also happens to be a valid base64 string.
            if isinstance(value, int) or _DECIMAL_STRING_RE.fullmatch(value):
                return _decimal_from_number(value, precision=precision, scale=scale)
            # Confluent base64 bytes path: strings that are not numeric literals.
            try:
                raw = base64.b64decode(value, validate=True)
            except ValueError as e:
                raise InvalidPayload(
                    f"{value!r} is not a valid decimal value: expected a numeric string "
                    f'(e.g. "14.36") or base64-encoded unscaled bytes'
                ) from e
            unscaled = int.from_bytes(raw, byteorder="big", signed=True)
            converted = decimal.Decimal(unscaled).scaleb(-scale)
            if not _decimal_fits_precision(converted, precision=precision, scale=scale):
                raise InvalidPayload(f"{value!r} decodes to a decimal that does not fit schema precision {precision}")
            return converted

    return value


def read_value(config: Config, schema: TypedSchema, bio: io.BytesIO, avro_reader: DatumReader | None = None):
    if schema.schema_type is SchemaType.AVRO:
        reader = avro_reader if avro_reader is not None else DatumReader(writers_schema=schema.schema)
        return _jsonify_avro_payload(reader.read(BinaryDecoder(bio)))
    if schema.schema_type is SchemaType.JSONSCHEMA:
        value = json_decode(bio)
        try:
            schema.schema.validate(value)
        except ValidationError as e:
            raise InvalidPayload from e
        return value

    if schema.schema_type is SchemaType.PROTOBUF:
        try:
            reader = ProtobufDatumReader(config, schema.schema)
            return reader.read(bio)
        except DecodeError as e:
            raise InvalidPayload from e

    raise ValueError("Unknown schema type")


def write_value(config: Config, schema: TypedSchema, bio: io.BytesIO, value: dict) -> None:
    if schema.schema_type is SchemaType.AVRO:
        if config.rest_avro_permissive_json_parser:
            # Backwards compatibility: Support JSON encoded data without the tags for unions.
            # First, try to convert logical types on the original value. If the resulting
            # value validates against the schema, use it as-is to preserve backwards
            # compatibility with existing union encodings. Otherwise, fall back to
            # flattening unions and then converting logical types.
            converted = convert_logical_types(schema.schema, value, config.rest_avro_extended_json_parser)
            if avro.io.validate(schema.schema, converted):
                data = converted
            else:
                flattened = flatten_unions(schema.schema, value)
                data = convert_logical_types(schema.schema, flattened, config.rest_avro_extended_json_parser)
        else:
            # Strict mode: only accept properly tagged union JSON.
            unfolded = _unfold_avro_json(
                schema.schema,
                value,
                config.rest_avro_extended_json_parser,
                path="records[0].value",
            )
            data = convert_logical_types(schema.schema, unfolded, config.rest_avro_extended_json_parser)
            if not avro.io.validate(schema.schema, data):
                raise InvalidPayload("records[0].value: value does not validate against Avro schema")

        writer = DatumWriter(writers_schema=schema.schema)
        writer.write(data, BinaryEncoder(bio))
    elif schema.schema_type is SchemaType.JSONSCHEMA:
        try:
            schema.schema.validate(value)
        except ValidationError as e:
            raise InvalidPayload from e
        bio.write(json_encode(value, binary=True))

    elif schema.schema_type is SchemaType.PROTOBUF:
        # TODO: PROTOBUF* we need use protobuf validator there
        writer = ProtobufDatumWriter(config, schema.schema)
        writer.write_index(bio)
        writer.write(value, bio)

    else:
        raise ValueError("Unknown schema type")
