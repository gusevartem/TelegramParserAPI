import logging
import sys

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from parser.settings import ProjectSettings


def _otel_trace_processor(
    _logger: structlog.typing.WrappedLogger,
    _name: str,
    event_dict: structlog.typing.EventDict,
):
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["otel"] = {
            "trace_id": f"{ctx.trace_id:032x}",
            "span_id": f"{ctx.span_id:016x}",
        }
    return event_dict


def setup_logging(settings: ProjectSettings, service_name: str, service_version: str):
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": service_version,
            "deployment.environment": "production"
            if not settings.debug
            else "development",
        }
    )

    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)

    if settings.trace_exporter_endpoint:
        trace_exporter = OTLPSpanExporter(endpoint=settings.trace_exporter_endpoint)
        tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))

    if settings.debug:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(key="ts", fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.dict_tracebacks,
                _otel_trace_processor,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    else:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(key="ts", fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.dict_tracebacks,
                _otel_trace_processor,
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        if settings.log_exporter_endpoint:
            logger_provider = LoggerProvider(resource=resource)
            exporter = OTLPLogExporter(endpoint=settings.log_exporter_endpoint)
            logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

            root = logging.getLogger()
            root.setLevel(logging.NOTSET)

            otel_handler = LoggingHandler(
                level=logging.NOTSET, logger_provider=logger_provider
            )
            root.addHandler(otel_handler)

            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setFormatter(logging.Formatter("%(message)s"))
            root.addHandler(stdout_handler)
