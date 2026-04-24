"""
External AVRO schema normalizer client.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dataclasses import dataclass
from karapace.core.config import Config
from karapace.core.utils import JSONDecodeError, json_decode, json_encode

import aiohttp
import async_timeout
import logging

LOG = logging.getLogger(__name__)
INVALID_SCHEMA_ERROR_CODE = 42201


@dataclass(frozen=True)
class ExternalSchemaNormalizerError(Exception):
    """Transport-level or service-level error returned by the normalizer."""

    status_code: int
    detail: dict[str, object]


class ExternalAvroSchemaNormalizer:
    """Client for an external AVRO normalization service.

    Contract:
      - request: {"subject": "<subject>", "schema": "<schema_string>"}
      - success: {"schema": "<normalized_schema>", "normalized": <bool>, ...}
      - error:   {"error_code": ..., "message": "...", ...}
    """

    def __init__(self, config: Config) -> None:
        self._enabled = config.external_avro_normalizer_global_enabled
        self._url = config.external_avro_normalizer_url
        self._timeout_seconds = max(1, config.external_avro_normalizer_timeout_ms) / 1000.0
        self._client: aiohttp.ClientSession | None = aiohttp.ClientSession() if self._enabled else None

    async def close(self) -> None:
        """Close the HTTP client if it has been initialized."""
        if self._client is not None:
            await self._client.close()

    @property
    def enabled(self) -> bool:
        """Return feature flag status for external AVRO normalization."""
        return self._enabled

    async def normalize_schema(self, *, subject: str, schema_str: str) -> str:
        """Normalize schema string using the external service.

        Raises ExternalSchemaNormalizerError with status/details that are safe
        to proxy to API clients.
        """
        if self._url is None:
            raise ExternalSchemaNormalizerError(
                status_code=500,
                detail={
                    "error_code": "external_avro_normalizer_misconfigured",
                    "message": "External AVRO normalizer URL is not configured",
                },
            )
        if self._client is None:
            raise ExternalSchemaNormalizerError(
                status_code=500,
                detail={
                    "error_code": "external_avro_normalizer_not_initialized",
                    "message": "External AVRO normalizer HTTP client is not initialized",
                },
            )

        payload = {"subject": subject, "schema": schema_str}

        try:
            async with async_timeout.timeout(self._timeout_seconds):
                async with self._client.post(
                    self._url,
                    data=json_encode(payload),
                    headers={"Content-Type": "application/json"},
                ) as response:
                    response_body = await response.text()
                    if response.status >= 400:
                        mapped_status, mapped_detail = self._error_from_response(
                            status=response.status, response_body=response_body
                        )
                        raise ExternalSchemaNormalizerError(
                            status_code=mapped_status,
                            detail=mapped_detail,
                        )
                    return self._normalized_schema_from_success(response_body=response_body)
        except ExternalSchemaNormalizerError:
            raise
        except TimeoutError as exc:
            raise ExternalSchemaNormalizerError(
                status_code=504,
                detail={"error_code": "external_avro_normalizer_timeout", "message": "External AVRO normalizer timed out"},
            ) from exc
        except aiohttp.ClientError as exc:
            LOG.warning("External AVRO normalizer transport error: %s", exc)
            raise ExternalSchemaNormalizerError(
                status_code=502,
                detail={
                    "error_code": "external_avro_normalizer_unreachable",
                    "message": f"External AVRO normalizer request failed: {exc}",
                },
            ) from exc

    def _normalized_schema_from_success(self, *, response_body: str) -> str:
        """Validate success payload shape and return normalized schema string."""
        try:
            response_json = json_decode(response_body, assume_type=dict)
        except (JSONDecodeError, TypeError, ValueError) as exc:
            raise ExternalSchemaNormalizerError(
                status_code=502,
                detail={
                    "error_code": "external_avro_normalizer_invalid_response",
                    "message": "External AVRO normalizer returned invalid JSON response",
                },
            ) from exc

        normalized_schema = response_json.get("schema")
        if not isinstance(normalized_schema, str):
            raise ExternalSchemaNormalizerError(
                status_code=502,
                detail={
                    "error_code": "external_avro_normalizer_invalid_response",
                    "message": "External AVRO normalizer response must include string field 'schema'",
                },
            )
        return normalized_schema

    def _error_from_response(self, *, status: int, response_body: str) -> tuple[int, dict[str, object]]:
        """Parse and map normalizer errors to Karapace error model."""
        try:
            response_json = json_decode(response_body, assume_type=dict)
            error_code = response_json.get("error_code", status)
            message = response_json.get("message", response_body or "Unknown error")
            details = response_json.get("details")
            mapped_status = self._map_upstream_error_status(
                status=status,
                error_code=error_code,
                message=message,
                details=details,
            )
            mapped_error_code: str | int = error_code
            if mapped_status == 422:
                mapped_error_code = INVALID_SCHEMA_ERROR_CODE

            detail: dict[str, object] = {"error_code": mapped_error_code, "message": message}
            if details is not None:
                detail["details"] = details
            return mapped_status, detail
        except (JSONDecodeError, TypeError, ValueError):
            return status, {"error_code": status, "message": response_body or "Unknown error"}

    def _map_upstream_error_status(
        self,
        *,
        status: int,
        error_code: object,
        message: object,
        details: object,
    ) -> int:
        """Map upstream validation errors to Karapace status conventions."""
        if status >= 500:
            return status
        if status == 422:
            return 422
        if status != 400:
            return status
        if self._is_schema_semantic_error(error_code=error_code, message=message, details=details):
            return 422
        return 400

    def _is_schema_semantic_error(self, *, error_code: object, message: object, details: object) -> bool:
        schema_tokens = ("schema", "avro", "parse", "validation", "incompatible", "invalid field")
        request_tokens = (
            "subject is required",
            "missing subject",
            "missing required",
            "malformed",
            "request",
            "payload",
            "json",
        )

        code_text = str(error_code).lower()
        if "schema" in code_text or "avro" in code_text:
            return True

        message_text = str(message).lower()
        if any(token in message_text for token in schema_tokens):
            if not any(token in message_text for token in request_tokens):
                return True
        if any(token in message_text for token in ("invalid schema", "schema is invalid", "failed to parse")):
            return True

        if isinstance(details, dict):
            details_text = json_encode(details).lower()
            if any(token in details_text for token in schema_tokens):
                return True

        return False
