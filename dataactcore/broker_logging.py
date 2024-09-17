import logging.config
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.urllib import URLLibInstrumentor

from dataactcore.config import CONFIG_BROKER
from dataactcore.config import CONFIG_LOGGING


def deep_merge(left, right):
    """Deep merge dictionaries, replacing values from right"""
    if isinstance(left, dict) and isinstance(right, dict):
        result = left.copy()
        for key in right:
            if key in left:
                result[key] = deep_merge(left[key], right[key])
            else:
                result[key] = right[key]
        return result
    else:
        return right


# Reasonable defaults to avoid clutter in our config files
DEFAULT_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': "%(asctime)s %(levelname)s:%(name)s:%(message)s"
        },
    },
    'handlers': {
        'console': {
            'formatter': 'default',
            'class': 'logging.StreamHandler'
        },
    },
    'loggers': {
        # i.e. "all modules"
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True
        },
        'dataactbroker': {
            'level': 'DEBUG',
            'propagate': True
        },
        'dataactcore': {
            'level': 'DEBUG',
            'propagate': True
        },
        'dataactvalidator': {
            'level': 'DEBUG',
            'propagate': True
        },
        '__main__': {  # for the __main__ module within /dataactvalidator/app.py
            'level': 'DEBUG',
            'propagate': True
        },
    }
}


def configure_logging(service_name='broker'):
    resource = Resource.create(attributes={"service.name": service_name})

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    if CONFIG_BROKER['local']:
        exporter = ConsoleSpanExporter()
    else:
        exporter = OTLPSpanExporter(
            endpoint=os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
        )
    span_processor = BatchSpanProcessor(exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    LoggingInstrumentor().instrument(tracer_provider=trace.get_tracer_provider(), set_logging_format=True)
    URLLibInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())

    config = DEFAULT_CONFIG
    if 'python_config' in CONFIG_LOGGING:
        config = deep_merge(config, CONFIG_LOGGING['python_config'])
    logging.config.dictConfig(config)

    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
