"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

Unit tests for the REST proxy topic WRITE ACL pre-check:

- :class:`karapace.kafka_rest_apis.authorization_cache.TopicWriteAclCache`
- ``UserRestProxy._enforce_topic_write_acl`` branches (no-op, allow, 403,
  404, 500).
- ``validate_config`` rejecting ``rest_authorization_enforce_topic_write``
  without ``rest_authorization``.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka.errors import UnknownTopicOrPartitionError
from confluent_kafka.admin import AclOperation

from karapace.core.config import InvalidConfiguration, validate_config
from karapace.core.container import KarapaceContainer
from karapace.kafka_rest_apis import UserRestProxy
from karapace.kafka_rest_apis.authorization_cache import TopicWriteAclCache
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.rapu import HTTPResponse


# ---------------------------------------------------------------------------
# TopicWriteAclCache
# ---------------------------------------------------------------------------


async def test_cache_returns_cached_value_without_refetching() -> None:
    fetcher = AsyncMock(return_value=True)
    cache = TopicWriteAclCache(fetcher=fetcher, ttl_s=60, maxsize=16)

    assert await cache.is_write_allowed("topic-a") is True
    assert await cache.is_write_allowed("topic-a") is True
    assert await cache.is_write_allowed("topic-a") is True

    fetcher.assert_awaited_once_with("topic-a")


async def test_cache_coalesces_concurrent_calls_for_same_topic() -> None:
    gate = asyncio.Event()
    call_count = 0

    async def slow_fetcher(topic: str) -> bool:
        nonlocal call_count
        call_count += 1
        await gate.wait()
        return True

    cache = TopicWriteAclCache(fetcher=slow_fetcher, ttl_s=60, maxsize=16)

    tasks = [asyncio.create_task(cache.is_write_allowed("topic-a")) for _ in range(5)]
    # Let the owner enter the fetcher and waiters queue up behind the event.
    await asyncio.sleep(0.05)
    gate.set()
    results = await asyncio.gather(*tasks)

    assert results == [True, True, True, True, True]
    assert call_count == 1


async def test_cache_differentiates_topics() -> None:
    async def fetcher(topic: str) -> bool:
        return topic == "allowed"

    cache = TopicWriteAclCache(fetcher=fetcher, ttl_s=60, maxsize=16)

    assert await cache.is_write_allowed("allowed") is True
    assert await cache.is_write_allowed("denied") is False


async def test_cache_caches_negative_decisions() -> None:
    fetcher = AsyncMock(return_value=False)
    cache = TopicWriteAclCache(fetcher=fetcher, ttl_s=60, maxsize=16)

    assert await cache.is_write_allowed("topic-a") is False
    assert await cache.is_write_allowed("topic-a") is False

    fetcher.assert_awaited_once_with("topic-a")


async def test_cache_ttl_expiry_triggers_refetch() -> None:
    fetcher = AsyncMock(side_effect=[True, False])
    cache = TopicWriteAclCache(fetcher=fetcher, ttl_s=1, maxsize=16)

    assert await cache.is_write_allowed("topic-a") is True
    # Manually expire the entry without waiting on wall-clock time.
    cache.invalidate("topic-a")
    assert await cache.is_write_allowed("topic-a") is False

    assert fetcher.await_count == 2


async def test_cache_recovers_when_owner_fetch_raises() -> None:
    call_count = 0

    async def fetcher(topic: str) -> bool:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("broker down")
        return True

    cache = TopicWriteAclCache(fetcher=fetcher, ttl_s=60, maxsize=16)

    with pytest.raises(RuntimeError):
        await cache.is_write_allowed("topic-a")

    # Subsequent call should re-run the fetcher rather than return a stale value.
    assert await cache.is_write_allowed("topic-a") is True
    assert call_count == 2


def test_decision_from_operations_empty_means_deny() -> None:
    assert TopicWriteAclCache.decision_from_operations(frozenset()) is False


def test_decision_from_operations_without_write_denies() -> None:
    operations = frozenset({AclOperation.READ, AclOperation.DESCRIBE})
    assert TopicWriteAclCache.decision_from_operations(operations) is False


def test_decision_from_operations_with_write_allows() -> None:
    operations = frozenset({AclOperation.READ, AclOperation.WRITE})
    assert TopicWriteAclCache.decision_from_operations(operations) is True


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


def test_validate_config_requires_rest_authorization_for_topic_write_enforcement(
    karapace_container: KarapaceContainer,
) -> None:
    with pytest.raises(InvalidConfiguration):
        karapace_container.config().set_config_defaults(
            {
                "rest_authorization": False,
                "rest_authorization_enforce_topic_write": True,
            }
        )


def test_validate_config_accepts_topic_write_enforcement_with_rest_authorization(
    karapace_container: KarapaceContainer,
) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
            "rest_authorization_enforce_topic_write": True,
        }
    )
    # Redundant second call: ensures the standalone validator is also happy.
    validate_config(config)


def test_validate_config_rejects_non_positive_ttl(karapace_container: KarapaceContainer) -> None:
    with pytest.raises(InvalidConfiguration):
        karapace_container.config().set_config_defaults(
            {
                "rest_authorization": True,
                "sasl_bootstrap_uri": "localhost:9094",
                "rest_authorization_enforce_topic_write": True,
                "rest_authorization_topic_acl_cache_ttl_s": 0,
            }
        )


def test_validate_config_rejects_non_positive_maxsize(karapace_container: KarapaceContainer) -> None:
    with pytest.raises(InvalidConfiguration):
        karapace_container.config().set_config_defaults(
            {
                "rest_authorization": True,
                "sasl_bootstrap_uri": "localhost:9094",
                "rest_authorization_enforce_topic_write": True,
                "rest_authorization_topic_acl_cache_max_size": 0,
            }
        )


# ---------------------------------------------------------------------------
# UserRestProxy._enforce_topic_write_acl
# ---------------------------------------------------------------------------


def _make_proxy_with_cache(cache: TopicWriteAclCache | None) -> UserRestProxy:
    """Build a bare ``UserRestProxy`` without going through ``__init__``.

    ``UserRestProxy.__init__`` creates network clients and an admin client;
    for unit-testing the enforcement helper we only need the ACL cache
    attribute, so we bypass construction and wire just the pieces under test.
    """
    proxy = UserRestProxy.__new__(UserRestProxy)
    proxy._topic_write_acl_cache = cache  # type: ignore[attr-defined]
    return proxy


async def test_enforce_is_noop_when_cache_disabled() -> None:
    proxy = _make_proxy_with_cache(None)
    await proxy._enforce_topic_write_acl("orders", "application/json")


async def test_enforce_allows_when_cache_returns_true() -> None:
    cache = MagicMock(spec=TopicWriteAclCache)
    cache.is_write_allowed = AsyncMock(return_value=True)
    proxy = _make_proxy_with_cache(cache)
    await proxy._enforce_topic_write_acl("orders", "application/json")
    cache.is_write_allowed.assert_awaited_once_with("orders")


async def test_enforce_raises_403_when_cache_returns_false() -> None:
    cache = MagicMock(spec=TopicWriteAclCache)
    cache.is_write_allowed = AsyncMock(return_value=False)
    proxy = _make_proxy_with_cache(cache)

    with pytest.raises(HTTPResponse) as excinfo:
        await proxy._enforce_topic_write_acl("orders", "application/json")

    assert excinfo.value.status == HTTPStatus.FORBIDDEN
    assert excinfo.value.body["error_code"] == RESTErrorCodes.TOPIC_AUTHORIZATION_FAILED.value


async def test_enforce_raises_404_on_unknown_topic() -> None:
    cache = MagicMock(spec=TopicWriteAclCache)
    cache.is_write_allowed = AsyncMock(side_effect=UnknownTopicOrPartitionError())
    proxy = _make_proxy_with_cache(cache)

    with pytest.raises(HTTPResponse) as excinfo:
        await proxy._enforce_topic_write_acl("missing", "application/json")

    assert excinfo.value.status == HTTPStatus.NOT_FOUND
    assert excinfo.value.body["error_code"] == RESTErrorCodes.TOPIC_NOT_FOUND.value


async def test_enforce_raises_500_on_unexpected_error() -> None:
    cache = MagicMock(spec=TopicWriteAclCache)
    cache.is_write_allowed = AsyncMock(side_effect=RuntimeError("broker unreachable"))
    proxy = _make_proxy_with_cache(cache)

    with pytest.raises(HTTPResponse) as excinfo:
        await proxy._enforce_topic_write_acl("orders", "application/json")

    assert excinfo.value.status == HTTPStatus.INTERNAL_SERVER_ERROR
    assert excinfo.value.body["error_code"] == RESTErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value
