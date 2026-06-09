"""
karapace - utils

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from .typing import ArgJsonData, JsonData
from aiohttp.web_log import AccessLogger
from aiohttp.web_request import BaseRequest
from aiohttp.web_response import StreamResponse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import AnyStr, cast, IO, Literal, NoReturn, overload, TypeVar

import datetime as _dt_module
import importlib.util
import logging
import signal
import time

# Try orjson first (fastest), then ujson (fast), then json (fallback)
if importlib.util.find_spec("orjson"):
    from json import JSONDecodeError  # noqa: F401

    import orjson

    class _JsonModule:
        """Wrapper to make orjson API compatible with json/ujson."""

        @staticmethod
        def loads(s: bytes | str):
            """Load JSON from string or bytes."""
            if isinstance(s, str):
                s = s.encode("utf-8")
            return orjson.loads(s)

        @staticmethod
        def dumps(obj, *, default=None, indent=None, sort_keys=False, separators=None, passthrough_datetime=False, **kwargs):
            """Dump object to JSON string (returns str for compatibility).

            When ``passthrough_datetime`` is True, datetime/date/time objects are routed
            through ``default`` (e.g. for "Z"-suffixed timestamps of consumed Avro
            logical types) instead of orjson's native RFC 3339 serialization.
            """
            options = orjson.OPT_PASSTHROUGH_DATETIME if passthrough_datetime else 0
            if sort_keys:
                options |= orjson.OPT_SORT_KEYS
            if indent is not None:
                # orjson only supports indent=2
                options |= orjson.OPT_INDENT_2
            # orjson doesn't support custom separators, they're always compact or indented
            result = orjson.dumps(obj, default=default, option=options)
            return result.decode("utf-8")

        @staticmethod
        def load(fp):
            """Load JSON from file object."""
            content = fp.read()
            if isinstance(content, str):
                content = content.encode("utf-8")
            return orjson.loads(content)

        @staticmethod
        def dump(
            obj, fp, *, default=None, indent=None, sort_keys=False, separators=None, passthrough_datetime=False, **kwargs
        ):
            """Dump object to JSON file."""
            options = orjson.OPT_PASSTHROUGH_DATETIME if passthrough_datetime else 0
            if sort_keys:
                options |= orjson.OPT_SORT_KEYS
            if indent is not None:
                options |= orjson.OPT_INDENT_2
            result = orjson.dumps(obj, default=default, option=options)
            fp.write(result)

    json = _JsonModule()
    _JSON_BACKEND_SUPPORTS_PASSTHROUGH_DATETIME = True

elif importlib.util.find_spec("ujson"):
    from ujson import JSONDecodeError  # noqa: F401

    import ujson as _ujson

    class _JsonModule:  # type: ignore[no-redef]
        """Wrapper around ujson that can route datetime/date/time through ``_isoformat``.

        ujson serialises datetime objects natively as "+00:00" offset strings, bypassing
        the ``default`` callback (which is only invoked for *unknown* types).  When
        ``passthrough_datetime`` is requested, we keep the output format consistent with
        the stdlib-json backend (which calls ``_isoformat`` and produces "Z") by
        recursively replacing datetime/date/time objects before handing the value to
        ujson.  Without the flag ujson's native behaviour is preserved.
        """

        @staticmethod
        def _preprocess(obj):
            """Recursively convert datetime/date/time so ujson never sees them natively."""
            if isinstance(obj, datetime):
                return _isoformat(obj)
            if isinstance(obj, (date, _dt_module.time)):
                return obj.isoformat()
            if isinstance(obj, dict):
                return {k: _JsonModule._preprocess(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_JsonModule._preprocess(v) for v in obj]
            return obj

        @staticmethod
        def loads(s):
            return _ujson.loads(s)

        @staticmethod
        def dumps(obj, *, default=None, indent=None, sort_keys=False, separators=None, passthrough_datetime=False, **kwargs):
            if passthrough_datetime:
                obj = _JsonModule._preprocess(obj)
            return _ujson.dumps(obj, default=default, indent=indent or 0, sort_keys=sort_keys)

        @staticmethod
        def load(fp):
            return _ujson.load(fp)

        @staticmethod
        def dump(
            obj, fp, *, default=None, indent=None, sort_keys=False, separators=None, passthrough_datetime=False, **kwargs
        ):
            if passthrough_datetime:
                obj = _JsonModule._preprocess(obj)
            return _ujson.dump(obj, fp, default=default, indent=indent or 0, sort_keys=sort_keys)

    json = _JsonModule()
    _JSON_BACKEND_SUPPORTS_PASSTHROUGH_DATETIME = True
else:
    from json import JSONDecodeError  # noqa: F401

    import json

    # The stdlib backend has no native datetime support: such values always go through
    # the ``default`` callback, which is equivalent to passthrough being enabled.
    _JSON_BACKEND_SUPPORTS_PASSTHROUGH_DATETIME = False

NS_BLACKOUT_DURATION_SECONDS = 120
LOG = logging.getLogger(__name__)


def _isoformat(datetime_obj: datetime) -> str:
    """Return datetime to ISO 8601 variant suitable for users.

    Assume UTC for datetime objects without a timezone, always use the Z
    timezone designator.
    """
    if datetime_obj.tzinfo:
        datetime_obj = datetime_obj.astimezone(timezone.utc).replace(tzinfo=None)
    return datetime_obj.isoformat() + "Z"


@overload
def default_json_serialization(obj: datetime) -> str: ...


@overload
def default_json_serialization(obj: timedelta) -> float: ...


@overload
def default_json_serialization(obj: Decimal) -> str: ...


@overload
def default_json_serialization(obj: date) -> str: ...


@overload
def default_json_serialization(obj: _dt_module.time) -> str: ...


@overload
def default_json_serialization(obj: MappingProxyType) -> dict: ...


def default_json_serialization(
    obj: datetime | timedelta | Decimal | date | _dt_module.time | MappingProxyType,
) -> str | float | dict:
    if isinstance(obj, datetime):
        return _isoformat(obj)
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, _dt_module.time):
        return obj.isoformat()
    if isinstance(obj, MappingProxyType):
        return dict(obj)

    assert_never(f"Object of type {obj.__class__.__name__!r} is not JSON serializable")


@overload
def json_encode(
    obj: ArgJsonData,
    *,
    sort_keys: bool | None = ...,
    compact: bool | None = ...,
    indent: int | None = ...,
    passthrough_datetime: bool = ...,
) -> str: ...


@overload
def json_encode(
    obj: ArgJsonData,
    *,
    binary: Literal[True] = ...,
    sort_keys: bool | None = ...,
    compact: bool | None = ...,
    indent: int | None = ...,
    passthrough_datetime: bool = ...,
) -> bytes: ...


def json_encode(
    obj: ArgJsonData,
    *,
    binary: bool = False,
    sort_keys: bool | None = None,
    compact: bool | None = None,
    indent: int | None = None,
    passthrough_datetime: bool = False,
) -> AnyStr:
    """Encode ``obj`` as JSON.

    ``passthrough_datetime`` opts in to routing datetime/date/time objects through
    ``default_json_serialization`` (producing "Z"-suffixed UTC timestamps). It should
    only be enabled where such objects are expected in the payload, e.g. consumed
    Avro records with logical types. By default the backend-native datetime
    serialization is used.
    """
    kwargs = {}
    if indent is not None:
        kwargs["indent"] = indent
    if compact is not False and indent is None:
        kwargs["separators"] = (",", ":")
    if sort_keys is True:
        kwargs["sort_keys"] = True
    if passthrough_datetime and _JSON_BACKEND_SUPPORTS_PASSTHROUGH_DATETIME:
        kwargs["passthrough_datetime"] = True
    result = json.dumps(obj, default=default_json_serialization, **kwargs)
    return result.encode("utf8") if binary is True else result


T = TypeVar("T")


@overload
def json_decode(content: AnyStr | IO[AnyStr]) -> JsonData: ...


@overload
def json_decode(content: AnyStr | IO[AnyStr], assume_type: type[T]) -> T: ...


def json_decode(
    content: AnyStr | IO[AnyStr],
    # This argument is only used to pass onto cast() via a type var, it has no runtime
    # usage.
    assume_type: type[T] | None = None,
) -> JsonData | T:
    if isinstance(content, (str, bytes)):
        return cast("T | None", json.loads(content))
    return cast("T | None", json.load(content))


def assert_never(value: NoReturn) -> NoReturn:
    raise RuntimeError(f"This code should never be reached, got: {value}")


def get_project_root() -> Path:
    return Path(__file__).parent.parent


class Timeout(Exception):
    pass


@dataclass(frozen=True)
class Expiration:
    start_time: float
    deadline: float

    @classmethod
    def from_timeout(cls, timeout: float) -> Expiration:
        start_time = time.monotonic()
        deadline = start_time + timeout
        return cls(start_time, deadline)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    def is_expired(self) -> bool:
        return time.monotonic() > self.deadline

    def raise_timeout_if_expired(self, msg_format: str, *args: object, **kwargs: object) -> None:
        """Raise `Timeout` if this object is expired.

        Note:
            This method is supposed to be used in a loop, e.g.:

                expiration = Expiration.from_timeout(timeout=60)
                while is_data_ready(data):
                    expiration.raise_timeout_if_expired("something about", data)
                    data = gather_data()

            The exception message should be meaningful, so it may format data
            into the message itself. However formatting is expensive and should
            be done only when the deadline is expired, so this uses a similar
            interface to `logging.<level>()`.
        """
        if self.is_expired():
            raise Timeout(msg_format.format(*args, **kwargs))


class DebugAccessLogger(AccessLogger):
    """
    Logs access logs as DEBUG instead of INFO.
    Source: https://github.com/aio-libs/aiohttp/blob/d01e257da9b37c35c68b3931026a2d918c271446/aiohttp/web_log.py#L191-L210
    """

    def log(
        self,
        request: BaseRequest,
        response: StreamResponse,
        time: float,
    ) -> None:
        try:
            fmt_info = self._format_line(request, response, time)

            values = list()
            extra = dict()
            for key, value in fmt_info:
                values.append(value)

                if key.__class__ is str:
                    extra[key] = value
                else:
                    k1, k2 = key
                    dct = extra.get(k1, {})
                    dct[k2] = value
                    extra[k1] = dct

            self.logger.debug(self._log_format % tuple(values), extra=extra)
        except Exception:
            self.logger.exception("Error in logging")


def shutdown():
    """
    Send a SIGTERM into the current running application process, which should initiate shutdown logic.
    """
    LOG.warning("=======> Sending shutdown signal `SIGTERM` to Application process <=======")
    signal.raise_signal(signal.SIGTERM)
