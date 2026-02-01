import logging
import sys

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
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
    if span.is_recording():
        attributes: dict[str, str | int | float | bool] = {}
        for k, v in event_dict.items():
            if (
                k != "event"
                and k != "level"
                and (
                    isinstance(v, str)
                    or isinstance(v, int)
                    or isinstance(v, float)
                    or isinstance(v, bool)
                    or v is None
                )
            ):
                attributes[k] = v or "none"
        span.add_event(
            name=event_dict.get("event", ""),
            attributes={
                "log.level": event_dict.get("level", "info"),
                **attributes,
            },
        )

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

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(key="ts", fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _otel_trace_processor,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(stdout_handler)
