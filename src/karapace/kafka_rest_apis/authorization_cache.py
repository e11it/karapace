"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

Per-user Kafka topic ACL cache used by the REST proxy to avoid issuing
a ``describe_topics`` RPC on every publish request.

The cache is intentionally scoped to a single :class:`UserRestProxy`
instance, which in turn is already scoped to a single authenticated
principal (see ``KafkaRest.get_user_proxy`` in
``src/karapace/kafka_rest_apis/__init__.py``). Therefore the cache key is
just the topic name - the principal is implicit.

Concurrency:

- The cache is async-safe: concurrent calls for the same topic coalesce
  into a single in-flight RPC through a per-topic ``asyncio.Event``.
- Negative results (``WRITE`` not allowed) are cached with the same TTL as
  positive ones to avoid retry storms from clients that keep publishing
  into a topic they cannot write to.

The cache does NOT invalidate on ACL changes server-side; operators should
pick a TTL that matches their acceptable staleness (default 60s).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from cachetools import TTLCache
from confluent_kafka.admin import AclOperation

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CachedDecision:
    """A cached ACL lookup outcome.

    :ivar allowed_write: Whether ``AclOperation.WRITE`` is in the set of
        operations the principal may perform on the topic.
    :ivar stored_at: Monotonic timestamp when this decision was stored.
        Used only for logging and metrics; TTL eviction is handled by
        :class:`cachetools.TTLCache`.
    """

    allowed_write: bool
    stored_at: float


class TopicWriteAclCache:
    """Async-safe TTL cache for per-topic ``WRITE`` ACL decisions for a
    single Kafka principal.

    The ``fetcher`` callable is invoked on cache miss to produce a fresh
    decision. It must return ``True`` if the principal is allowed to write
    to the topic, ``False`` if not, and is expected to be a thin wrapper
    around
    :meth:`karapace.core.kafka.admin.KafkaAdminClient.describe_topic_authorized_operations`.

    :param fetcher: Async callable ``(topic) -> bool`` resolving the decision.
    :param ttl_s: Time to live in seconds for cached decisions (positive int).
    :param maxsize: Maximum number of topics cached before LRU-style eviction.
    """

    def __init__(
        self,
        fetcher: Callable[[str], Awaitable[bool]],
        *,
        ttl_s: int,
        maxsize: int,
    ) -> None:
        self._fetcher = fetcher
        self._cache: TTLCache[str, _CachedDecision] = TTLCache(maxsize=maxsize, ttl=ttl_s)
        # Per-topic coalescing events. An entry is present iff an RPC is
        # currently in flight for that topic. Waiters ``await`` the event
        # and then read the cache once it is set.
        self._locks: dict[str, asyncio.Event] = {}
        # Guards mutations of ``self._locks`` and the "check-set-own"
        # transition on cache miss. Kept short-lived on purpose.
        self._lock = asyncio.Lock()

    async def is_write_allowed(self, topic: str) -> bool:
        """Return whether the principal is allowed to produce to ``topic``.

        On cache hit, returns the cached decision immediately. On miss,
        coalesces concurrent callers on the same topic behind a single RPC:
        only the first caller runs the fetcher, the rest wait on an
        ``asyncio.Event`` and read the freshly-populated cache entry.

        :param topic: Topic name to check.
        :returns: ``True`` iff ``AclOperation.WRITE`` is reported by the
            broker for the principal owning this cache.
        :raises: Any exception raised by ``fetcher`` is propagated to the
            caller that triggered the fetch. Waiters for that same topic
            will see an empty cache and retry by calling the fetcher
            themselves (one of them becoming the new owner).
        """
        cached = self._cache.get(topic)
        if cached is not None:
            return cached.allowed_write

        async with self._lock:
            # Re-check the cache inside the lock: another coroutine may have
            # populated it between our first lookup and acquiring the lock.
            cached = self._cache.get(topic)
            if cached is not None:
                return cached.allowed_write
            event = self._locks.get(topic)
            if event is None:
                event = asyncio.Event()
                self._locks[topic] = event
                owner = True
            else:
                owner = False

        if not owner:
            await event.wait()
            cached = self._cache.get(topic)
            if cached is None:
                # The owner failed without populating the cache. Retry from
                # scratch so that one of the waiters takes over as the new
                # owner. Recursion depth is bounded by the number of
                # consecutive failures - we keep it simple.
                return await self.is_write_allowed(topic)
            return cached.allowed_write

        try:
            allowed = await self._fetcher(topic)
            self._cache[topic] = _CachedDecision(
                allowed_write=allowed,
                stored_at=time.monotonic(),
            )
            return allowed
        finally:
            async with self._lock:
                self._locks.pop(topic, None)
            event.set()

    def invalidate(self, topic: str) -> None:
        """Remove ``topic`` from the cache.

        Called from two places:

        * unit/integration tests, to force the next lookup to go through the
          fetcher;
        * ``UserRestProxy.produce_messages`` when the Kafka broker itself
          raises ``TopicAuthorizationFailedError`` for a record. That means
          our cached "allow" decision is now stale (typically because the
          principal's ACL was revoked after the pre-check completed). By
          dropping the entry we make sure the next publish attempt issues a
          fresh ``describe_topics`` RPC instead of happily returning the
          stale positive decision until
          ``rest_authorization_topic_acl_cache_ttl_s`` elapses.
        """
        self._cache.pop(topic, None)

    @staticmethod
    def decision_from_operations(operations: frozenset[AclOperation]) -> bool:
        """Translate a set of allowed ``AclOperation`` into a write-decision.

        A non-empty set is required for a positive decision: an empty set
        from the broker means "authorized operations unknown / not
        reported" (for example, an authorizer that does not implement
        KIP-430), and in that case we fail closed.

        :param operations: Set of operations reported by the broker.
        :returns: ``True`` iff ``AclOperation.WRITE`` is present and the
            set is non-empty.
        """
        return bool(operations) and AclOperation.WRITE in operations
