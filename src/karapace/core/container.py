"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.api.forward_client import ForwardClient
from karapace.core.config import Config
from karapace.core.external_schema_normalizer import ExternalAvroSchemaNormalizer
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation


class KarapaceContainer(containers.DeclarativeContainer):
    config = providers.Singleton(Config)
    forward_client = providers.Singleton(ForwardClient, config=config())
    external_avro_schema_normalizer = providers.Singleton(ExternalAvroSchemaNormalizer, config=config())
    prometheus = providers.Singleton(PrometheusInstrumentation)
