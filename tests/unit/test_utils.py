"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from karapace.core.utils import json_encode, shutdown
from unittest.mock import patch

import datetime
import decimal
import logging

UTC_DATETIME = datetime.datetime(2020, 9, 13, 12, 26, 40, tzinfo=datetime.timezone.utc)


def test_json_encode_datetime_native_by_default() -> None:
    """Without passthrough_datetime the JSON backend's native datetime format is used.

    orjson (the primary backend) serializes timezone-aware UTC datetimes with a
    "+00:00" offset. This is the project-wide default behaviour.
    """
    assert json_encode({"ts": UTC_DATETIME}) == '{"ts":"2020-09-13T12:26:40+00:00"}'


def test_json_encode_datetime_passthrough_uses_z_suffix() -> None:
    """passthrough_datetime routes datetimes through default_json_serialization ("Z" suffix)."""
    assert json_encode({"ts": UTC_DATETIME}, passthrough_datetime=True) == '{"ts":"2020-09-13T12:26:40Z"}'


def test_json_encode_datetime_passthrough_shifts_to_utc() -> None:
    """Timezone-aware datetimes are shifted to UTC before formatting with passthrough enabled."""
    helsinki_noon = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    assert json_encode({"ts": helsinki_noon}, passthrough_datetime=True) == '{"ts":"2000-01-01T10:00:00Z"}'


def test_json_encode_date_and_time_passthrough() -> None:
    value = {"d": datetime.date(2019, 4, 14), "t": datetime.time(12, 0, 0)}
    assert json_encode(value, passthrough_datetime=True) == '{"d":"2019-04-14","t":"12:00:00"}'


def test_json_encode_decimal_works_in_both_modes() -> None:
    """Decimal has no native backend support and always goes through the default callback."""
    value = {"price": decimal.Decimal("14.36")}
    assert json_encode(value) == '{"price":"14.36"}'
    assert json_encode(value, passthrough_datetime=True) == '{"price":"14.36"}'


def test_shutdown(caplog: LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="karapace.core.utils"):
        with patch("karapace.core.utils.signal") as mock_signal:
            mock_signal.SIGTERM = 15

            shutdown()
            mock_signal.raise_signal.assert_called_once_with(15)
            for log in caplog.records:
                assert log.name == "karapace.core.utils"
                assert log.levelname == "WARNING"
                assert log.message == "=======> Sending shutdown signal `SIGTERM` to Application process <======="
