"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

End-to-end regression coverage for the REST proxy topic WRITE ACL
pre-check. The feature normally requires ``rest_authorization: true`` plus a
SASL-enabled Kafka listener so that broker-side ACLs can evaluate the
caller's principal. Standing up that infrastructure in the existing
integration suite would be a separate, sizeable workstream.

To keep the test focused on the REST -> Schema Registry boundary the actual
broker ACL decision is simulated by injecting a custom
:class:`TopicWriteAclCache` into the live :class:`UserRestProxy` after it is
constructed. This lets us assert that:

1. A denied decision short-circuits the request with HTTP 403.
2. Schema Registry is NOT contacted (no ``<topic>-value`` subject appears
   there) when the pre-check denies the request.
3. An allowed decision leaves the legacy publish path untouched.
"""

from __future__ import annotations

import asyncio

from karapace.core.client import Client
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.kafka_rest_apis import KafkaRest
from karapace.kafka_rest_apis.authorization_cache import TopicWriteAclCache
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from tests.utils import (
    REST_HEADERS,
    new_topic,
    schema_avro_json,
    test_objects_avro,
    wait_for_topics,
)

NEW_TOPIC_TIMEOUT = 10


async def _prime_default_proxy(rest_async: KafkaRest, rest_async_client: Client) -> None:
    """Issue one request so ``KafkaRest`` lazily builds its default proxy.

    Without ``rest_authorization`` there is a single proxy keyed by the
    empty string; we need that proxy to exist before we can inject the
    simulated ACL cache into it.
    """
    res = await rest_async_client.get("/brokers")
    assert res.ok
    # The janitor task may still be a no-op, but getting the proxy guarantees
    # the instance is stored under ``rest_async.proxies[""]``.
    assert "" in rest_async.proxies


def _install_acl_cache(rest_async: KafkaRest, *, allow: bool) -> None:
    """Replace the default proxy's ACL cache with one whose fetcher returns
    ``allow`` for any topic."""

    async def fetcher(_topic: str) -> bool:
        return allow

    rest_async.proxies[""]._topic_write_acl_cache = TopicWriteAclCache(
        fetcher=fetcher,
        ttl_s=60,
        maxsize=16,
    )


async def _subject_exists(registry_async_client: Client, subject: str) -> bool:
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    return res.ok


async def test_topic_write_acl_precheck_denied_returns_403(
    rest_async: KafkaRest,
    rest_async_client: Client,
    registry_async_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    topic = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    await _prime_default_proxy(rest_async, rest_async_client)
    _install_acl_cache(rest_async, allow=False)

    payload = {
        "value_schema": schema_avro_json,
        "records": [{"value": obj} for obj in test_objects_avro],
    }

    res = await rest_async_client.post(f"/topics/{topic}", json=payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 403
    body = res.json()
    assert body["error_code"] == RESTErrorCodes.TOPIC_AUTHORIZATION_FAILED.value
    # The subject must NOT have been created - this is the whole point of
    # running the check before Schema Registry is contacted.
    # Give any in-flight SR request a moment to surface, then assert absence.
    await asyncio.sleep(0.1)
    assert not await _subject_exists(registry_async_client, f"{topic}-value")


async def test_topic_write_acl_precheck_allowed_preserves_publish(
    rest_async: KafkaRest,
    rest_async_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    topic = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    await _prime_default_proxy(rest_async, rest_async_client)
    _install_acl_cache(rest_async, allow=True)

    payload = {
        "value_schema": schema_avro_json,
        "records": [{"value": obj} for obj in test_objects_avro],
    }

    res = await rest_async_client.post(f"/topics/{topic}", json=payload, headers=REST_HEADERS["avro"])

    assert res.ok, res.json()
    body = res.json()
    assert "value_schema_id" in body
    assert len(body["offsets"]) == len(test_objects_avro)


async def test_topic_write_acl_precheck_disabled_does_not_short_circuit(
    rest_async: KafkaRest,
    rest_async_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    """Regression guard: without the feature flag the proxy has no ACL
    cache and behaves as before."""
    topic = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    await _prime_default_proxy(rest_async, rest_async_client)
    assert rest_async.proxies[""]._topic_write_acl_cache is None

    payload = {
        "value_schema": schema_avro_json,
        "records": [{"value": obj} for obj in test_objects_avro],
    }

    res = await rest_async_client.post(f"/topics/{topic}", json=payload, headers=REST_HEADERS["avro"])

    assert res.ok, res.json()
