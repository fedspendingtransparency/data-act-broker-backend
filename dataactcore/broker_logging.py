import logging.config
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan, SpanProcessor
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.urllib import URLLibInstrumentor

from dataactcore.config import CONFIG_BROKER, CONFIG_LOGGING


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


# CUSTOM Logging EXPORTER for debugging
class LoggingSpanProcessor(SpanProcessor):
    def on_end(self, span: ReadableSpan) -> None:
        trace_id = span.context.trace_id
        span_id = span.context.span_id
        logger = logging.getLogger(__name__)
        logger.info(f"Span ended: trace_id={trace_id}, span_id={span_id}, {span.name}_attributes={span.attributes}")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


# Reasonable defaults to avoid clutter in our config files
DEFAULT_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': "%(asctime)s %(levelname)s:%(name)s:%(message)s [span_id=%(otelSpanID)s trace_id=%(otelTraceID)s]"
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
    config = DEFAULT_CONFIG
    if 'python_config' in CONFIG_LOGGING:
        config = deep_merge(config, CONFIG_LOGGING['python_config'])
    logging.config.dictConfig(config)

    resource = Resource.create(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    # Set up the OTLP exporter
    # Check out https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/
    # for more exporter configuration
    if CONFIG_BROKER['local']:
        # if local print the traces to the console
        # exporter = ConsoleSpanExporter()

        # custom debug information
        exporter = LoggingSpanProcessor()
        # trace.get_tracer_provider().add_span_processor(logging_span_processor)

    else:
        # if prod or non-prod send trace information to the endpoint
        exporter = OTLPSpanExporter(
            endpoint=os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
        )
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))

    LoggingInstrumentor(logging_format="%(msg)s [span_id=%(otelSpanID)s trace_id=%(otelTraceID)s]")
    LoggingInstrumentor().instrument(tracer_provider=trace.get_tracer_provider(), set_logging_format=False)
    URLLibInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())

    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
