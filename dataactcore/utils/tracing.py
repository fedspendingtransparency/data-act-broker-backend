"""
Module for Application Performance Monitoring and distributed tracing tools and helpers.

Specifically leveraging the Grafana Open Telemetry tracing client.
"""

import logging

from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import Status, StatusCode

from typing import Optional, Callable

# The tracer provider should only be set up once, typically in the settings or a dedicated setup module
_logger = logging.getLogger(__name__)
tracer = trace.get_tracer_provider().get_tracer(__name__)


def _activate_trace_filter(filter_class: Callable) -> None:
    if not hasattr(tracer, "_filters"):
        _logger.warning("OpenTelemetry does not support direct filter activation on tracer")
    else:
        if tracer._filters:
            tracer._filters.append(filter_class())
        else:
            tracer._filters = [filter_class()]
        tracer.configure(settings={"FILTERS": tracer._filters})


class OpenTelemetryEagerlyDropTraceFilter:
    """
    A trace filter that eagerly drops a trace, by filtering it out before sending it on to the Server API.
    It uses the `self.EAGERLY_DROP_TRACE_KEY` as a sentinel value. If present within any span's tags, the whole
    trace that the span is part of will be filtered out before being sent to the server.
    """

    EAGERLY_DROP_TRACE_KEY = "EAGERLY_DROP_TRACE"

    @classmethod
    def activate(cls):
        _activate_trace_filter(cls)

    @classmethod
    def drop(cls, span: trace.Span):
        span.set_status(Status(StatusCode.ERROR))
        span.set_attribute(cls.EAGERLY_DROP_TRACE_KEY, True)

    def process_trace(self, trace):
        """Drop trace if any span attribute has tag with key 'EAGERLY_DROP_TRACE'"""
        return None if any(span.get_attribute(self.EAGERLY_DROP_TRACE_KEY) for span in trace) else trace


class SubprocessTrace:
    """A context manager class to handle of entry and exit things that need to be done to get spans published that are
    part of a Datadog trace which continues into a subprocess.

    This wraps some of the context management activity done in ``Span`` class, which is also a context manager.
    """

    def __init__(
        self,
        name: str,
        kind: SpanKind,
        service: str = None,
        can_drop_sample: bool = True,
        **tags,
    ) -> None:
        self.name = name
        self.service = service
        self.kind = kind
        self.can_drop_sample = can_drop_sample
        self.tags = tags
        self.span: Optional[trace.Span] = None

    def __enter__(self) -> trace.Span:
        self.span = tracer.start_span(name=self.name)
        for key, value in self.tags.items():
            self.span.set_attribute(key, value)
        return self.span

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            # This will call span.finish() which must be done before the queue is flushed in order to enqueue the
            # span data that is to be flushed (sent to the server)
            self.span.__exit__(exc_type, exc_val, exc_tb)
        finally:
            trace.get_tracer_provider().force_flush()


class OpenTelemetryLoggingTraceFilter:
    """Debugging utility filter that can log trace spans"""

    _log = logging.getLogger(f"{__name__}.OpenTelemetryLoggingTraceFilter")

    @classmethod
    def activate(cls):
        _activate_trace_filter(cls)

    def process_trace(self, trace):
        logged = False
        trace_id = "???"
        for span in trace:
            trace_id = span.context.trace_id or "???"
            if not span.get_attribute(OpenTelemetryEagerlyDropTraceFilter.EAGERLY_DROP_TRACE_KEY):
                logged = True
                self._log.info(f"----[SPAN#{trace_id}]" + "-" * 40 + f"\n{span}")
        if logged:
            self._log.info(f"====[END TRACE#{trace_id}]" + "=" * 35)
        return trace
