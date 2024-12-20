import inspect

import logging
from logging.handlers import QueueHandler
import multiprocessing as mp
import pytest

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter, SpanExportResult
from opentelemetry.sdk.resources import Resource

from dataactcore.utils.tracing import (
    OpenTelemetryEagerlyDropTraceFilter,
    OpenTelemetryLoggingTraceFilter,
    SubprocessTrace
)


class CustomConsoleSpanExporter(ConsoleSpanExporter):
    """ ConsoleSpanExporter's export (despite going to sys.stdout) doesn't get captured by capsys.
        This fixes that by calling the print from in here.
    """
    def export(self, spans):
        for span in spans:
            print(self.formatter(span))
        return SpanExportResult.SUCCESS


# Initialize the tracer provider and exporter for testing
provider = TracerProvider(resource=Resource.create({'service.name': 'test-service'}))
trace.set_tracer_provider(provider)
exporter = CustomConsoleSpanExporter()
span_processor = SimpleSpanProcessor(exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
tracer = trace.get_tracer_provider().get_tracer(__name__)

# Instrument logging
logger = logging.getLogger(__name__)


@pytest.fixture
def caplog(caplog):
    """A decorator (pattern) fixture around the pytest caplog fixture that adds the ability to temporarily alter
    loggers with propagate=False to True for duration of the test, so their output is propagated to the caplog log
    handler"""

    restore = []
    for logger in logging.Logger.manager.loggerDict.values():
        try:
            if not logger.propagate:
                logger.propagate = True
                restore += [logger]
        except AttributeError:
            pass
    yield caplog
    for logger in restore:
        logger.propagate = False


def test_logging_trace_spans_basic(caplog):
    # Enable log output for this logger for duration of this test
    caplog.set_level(logging.DEBUG)

    test = f"{inspect.stack()[0][3]}"
    with tracer.start_as_current_span(name=f"{test}_operation") as span:
        span_attributes = {"service.name": f"{test}_service", "resource.name": f"{test}_resource", "span.type": "TEST"}
        for k, v in span_attributes.items():
            span.set_attribute(k, v)
        trace_id = span.get_span_context().trace_id
        span_id = span.get_span_context().span_id
        logger.info(f"Test log message with trace id: {trace_id}")
        log_span_id = f"The corresponding span id: {span_id}"
        logger.warning(log_span_id)

    assert f"trace id: {trace_id}" in caplog.text, "trace_id not found in logging output"
    assert f"span id: {span_id}" in caplog.text, "span_id not found in logging output"


@pytest.mark.skip('OpenTelemetry doesn\'t support custom filters')
def test_drop_key_on_trace_spans(caplog, capsys):
    """ Test that traces that have any span with the key that marks them for dropping, are not logged, but those that
    do not have this marker, are still logged"""
    caplog.set_level(logging.DEBUG)
    test = f"{inspect.stack()[0][3]}"
    OpenTelemetryLoggingTraceFilter.activate()
    OpenTelemetryEagerlyDropTraceFilter.activate()
    with tracer.start_as_current_span(name=f"{test}_operation") as span:
        trace_id1 = span.get_span_context().trace_id
        span_id1 = span.get_span_context().span_id
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)
        # do things
        x = 2 ** 5
        thirty_two_squares = [m for m in map(lambda y: y ** 2, range(x))]
        assert thirty_two_squares[-1] == 961

        # Drop this span so it is not sent to the server, and not logged by the trace logger
        OpenTelemetryEagerlyDropTraceFilter.drop(span)

    # Do another trace, that is NOT dropped
    with tracer.start_as_current_span(name=f"{test}_operation2") as span2:
        trace_id2 = span2.get_span_context().trace_id
        span_id2 = span2.get_span_context().span_id
        test_msg2 = f"a second test message was logged during {test}"
        logger.warning(test_msg2)
        # do things
        x = 2 ** 7

    captured = capsys.readouterr()
    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"
    assert hex(span_id1) not in captured.out, "span marker still logged when should have been dropped"
    assert hex(trace_id1) not in captured.out, "trace marker still logged when should have been dropped"
    assert test_msg2 in caplog.text
    assert hex(span_id2) in captured.out, "span marker not found in logging output"
    assert hex(trace_id2) in captured.out, "trace marker not found in logging output"
    assert OpenTelemetryEagerlyDropTraceFilter.EAGERLY_DROP_TRACE_KEY not in captured.out


def test_logging_trace_spans(caplog, capsys):
    """Test the OpenTelemetryLoggingTraceFilter can actually capture trace span data in log output"""
    caplog.set_level(logging.DEBUG)
    OpenTelemetryLoggingTraceFilter.activate()

    test = f"{inspect.stack()[0][3]}"
    with tracer.start_as_current_span(name=f"{test}_operation") as span:
        span_attributes = {"service.name": f"{test}_service", "resource.name": f"{test}_resource", "span.type": "TEST"}
        for k, v in span_attributes.items():
            span.set_attribute(k, v)
        trace_id = span.get_span_context().trace_id
        span_id = span.get_span_context().span_id
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)
        # do things
        x = 2 ** 5
        thirty_two_squares = [m for m in map(lambda y: y ** 2, range(x))]
        assert thirty_two_squares[-1] == 961

    captured = capsys.readouterr()
    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"
    assert f'{trace_id:x}' in captured.out, "trace_id not found in logging output"
    assert f'{span_id:x}' in captured.out, "span_id not found in logging output"
    assert f"{test}_resource" in captured.out, "traced resource not found in logging output"


@pytest.mark.skip(
    "Still needs verification that subprocess spans work as intended. Note this does not affect subprocess logs."
)
def test_subprocess_trace(caplog, capsys):
    """Verify that spans created in subprocesses are written to the queue and then flushed to the server,
    when wrapped in the SubprocessTracer"""
    caplog.set_level(logging.DEBUG)
    # Enable log output for this logger for duration of this test
    test = f"{inspect.stack()[0][3]}"
    # And also send its output through a multiprocessing queue to surface logs from the subprocess
    log_queue = mp.Queue()
    OpenTelemetryLoggingTraceFilter._log.addHandler(QueueHandler(log_queue))
    OpenTelemetryLoggingTraceFilter.activate()

    subproc_test_msg = f"a test message was logged in a subprocess of {test}"
    state = mp.Queue()
    stop_sentinel = "-->STOP<--"

    with tracer.start_as_current_span(name=f"{test}_operation") as span:
        span_id = span.get_span_context().trace_id
        trace_id = span.get_span_context().trace_id
        test_msg = f"a test message was logged during {test}"
        logger.warning(test_msg)
        ctx = mp.get_context("fork")
        worker = ctx.Process(
            name=f"{test}_subproc",
            target=_do_things_in_subproc,
            args=(
                subproc_test_msg,
                state,
            ),
        )

        worker.start()
        worker.join(timeout=10)
        if worker.is_alive():
            worker.terminate()
            try:
                _drain_captured_log_queue(log_queue, stop_sentinel, caplog, force_immediate_stop=True)
            except Exception:
                print("Error draining captured log queue when handling subproc TimeoutError")
                pass
            raise mp.TimeoutError(f"subprocess {worker.name} did not complete in timeout")
        OpenTelemetryLoggingTraceFilter._log.warning(stop_sentinel)

    subproc_trace_id, subproc_span_id = state.get(block=True, timeout=10)
    captured = capsys.readouterr()
    assert test_msg in caplog.text, "caplog.text did not seem to capture logging output during test"
    assert hex(trace_id) in captured.out, "trace marker not found in logging output"
    assert hex(span_id) in captured.out, "span marker not found in logging output"
    assert subproc_trace_id == trace_id  # subprocess tracing should be a continuation of the trace in parent process

    _drain_captured_log_queue(log_queue, stop_sentinel, caplog)

    assert hex(subproc_trace_id) in captured.out, "trace marker not found in logging output"
    assert hex(subproc_span_id) in captured.out, "span marker not found in logging output"


def _drain_captured_log_queue(log_queue, stop_sentinel, caplog, force_immediate_stop=False):
    # Drain the queue and redirect DatadogLoggingTraceFilter log output to the caplog handler
    log_records = []
    draining = True
    while draining:
        while not log_queue.empty():
            log_record = log_queue.get(block=True, timeout=5)
            log_records.append(log_record)
        log_msgs = [r.getMessage() for r in log_records]
        if force_immediate_stop or stop_sentinel in log_msgs:  # check for sentinel, signaling end of queued records
            draining = False
    for log_record in log_records:
        if log_record.getMessage() != stop_sentinel:
            caplog.handler.handle(log_record)


def _do_things_in_subproc(subproc_test_msg, q: mp.Queue):
    test = f"{inspect.stack()[0][3]}"
    with SubprocessTrace(
        name=f"{test}_operation",
        service=f"{test}_service",
        subproc_test_msg=subproc_test_msg,
    ) as span:
        span_ids = (
            span.get_span_context().trace_id,
            span.get_span_context().span_id,
        )
        q.put(span_ids, block=True, timeout=5)
        logger.warning(subproc_test_msg)
        # do things
        x = 2 ** 5
        thirty_two_squares = [m for m in map(lambda y: y ** 2, range(x))]
        assert thirty_two_squares[-1] == 961
