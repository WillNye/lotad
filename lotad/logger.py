from __future__ import annotations

import json
import logging
import os

import structlog


def pretty_log(logger, method_name, event_dict):
    return_dict = {}

    for key, value in event_dict.items():
        if key != "event":
            key = f"\n  {key}"

        if isinstance(value, dict) or isinstance(value, list):
            value = "\n  ".join(str(json.dumps(value, indent=2)).split("\n"))

        return_dict[key] = value

    return return_dict


def configure_logger(logger_name, log_level):
    if not structlog.is_configured():
        running_on_lambda = os.getenv("AWS_LAMBDA_FUNCTION_NAME")
        renderer = (
            structlog.processors.JSONRenderer(serializer=json.dumps)
            if running_on_lambda == "aws_lambda"
            else structlog.dev.ConsoleRenderer()
        )
        structlog.configure(
            processors=[
                pretty_log,
                structlog.processors.add_log_level,
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper("%Y/%m/%d %H:%M:%S", utc=False),
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                renderer,
            ],
            wrapper_class=structlog.make_filtering_bound_logger(
                logging.getLevelName(log_level)
            ),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=False,
        )

    return structlog.get_logger(logger_name)


logger = configure_logger("lotad", os.getenv("LOG_LEVEL", "INFO"))
