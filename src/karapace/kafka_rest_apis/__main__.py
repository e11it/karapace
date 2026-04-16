"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dependency_injector.wiring import inject
from karapace import version as karapace_version
from karapace.core.container import KarapaceContainer
from karapace.core.logging_setup import configure_logging, log_config_without_secrets
from karapace.kafka_rest_apis import KafkaRest

import argparse
import karapace.core.instrumentation.tracer
import logging
import sys


@inject
def main() -> int:
    container = KarapaceContainer()
    # NOTE: ``karapace.core.instrumentation.tracer`` must be wired here too,
    # otherwise ``Tracer.get_tracer``'s ``Provide[...]`` default is never
    # resolved and any tracer-wrapped admin call (e.g. ``describe_topics``)
    # fails with ``AttributeError: 'Provide' object has no attribute 'tags'``.
    # This matches the wiring already done in ``src/karapace/__main__.py``.
    container.wire(modules=[__name__, karapace.core.instrumentation.tracer])

    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.parse_args()
    configure_logging(config=container.config())
    log_config_without_secrets(config=container.config())

    logging.info("\n%s\nStarting %s\n%s", ("=" * 100), "Starting Karapace Rest Proxy", ("=" * 100))
    app = KafkaRest(config=container.config())

    try:
        container.prometheus().setup_metrics(app=app)
        app.run()  # `close` will be called by the callback `close_by_app` set by `KarapaceBase`
    except Exception as ex:
        app.stats.unexpected_exception(ex=ex, where="karapace")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
