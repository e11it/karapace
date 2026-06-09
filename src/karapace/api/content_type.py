"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from accept_types import get_best_match
from email.message import Message
from fastapi import HTTPException, Request, status

import logging

LOG = logging.getLogger(__name__)

JSON_CONTENT_TYPE = "application/json"

SCHEMA_CONTENT_TYPES = [
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    JSON_CONTENT_TYPE,
    "application/octet-stream",
]
SCHEMA_ACCEPT_VALUES = [
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    JSON_CONTENT_TYPE,
]
SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def _unsupported_media_type() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        detail={
            "error_code": status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            "message": "HTTP 415 Unsupported Media Type",
        },
    )


def _not_acceptable() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_406_NOT_ACCEPTABLE,
        detail={
            "error_code": status.HTTP_406_NOT_ACCEPTABLE,
            "message": "HTTP 406 Not Acceptable",
        },
    )


def negotiate_schema_content_type(request: Request) -> str:
    """Validate Accept and Content-Type headers for schema-registry endpoints.

    Returns the negotiated response content type on success.
    Raises HTTPException 406 or 415 on invalid headers.
    """
    method = request.method
    content_type_header = request.headers.get("Content-Type")

    if method in {"POST", "PUT"} and not content_type_header:
        raise _unsupported_media_type()

    message = Message()
    message["Content-Type"] = content_type_header or JSON_CONTENT_TYPE
    params = message.get_params()
    assert params is not None
    # Media type and subtype are case-insensitive (RFC 7231 section 3.1.1.1).
    content_type = params[0][0].lower()

    if method in {"POST", "PUT"} and content_type not in SCHEMA_CONTENT_TYPES:
        raise _unsupported_media_type()
    accept_val = request.headers.get("Accept")
    if accept_val:
        if accept_val in ("*/*", "*") or accept_val.startswith("*/"):
            return SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE
        content_type_match = get_best_match(accept_val.lower(), SCHEMA_ACCEPT_VALUES)
        if not content_type_match:
            LOG.debug("Unexpected Accept value: %r", accept_val)
            raise _not_acceptable()
        return content_type_match
    return SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE
