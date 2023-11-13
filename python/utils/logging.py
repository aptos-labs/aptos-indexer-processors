"""
This module contains the custom logger and formatter for the application.

To use the custom logger, import the module and use the logger as follows:
    
        from utils.logging import CustomLogger
        logger = CustomLogger(__name__)
        logger.info("This is an info message", extra={"extra_field": "extra_value"})
The resulting log message will be in JSON format:
    {
        "timestamp": "2021-03-15T14:29:31.000Z",
        "level": "INFO",
        "fields": {
            "message": "This is an info message",
            "extra_field": "extra_value"
        },
        "module": "__main__",
        "func_name": "<module>",
        "path_name": "/Users/.../python/utils/logging.py",
        "line_no": 41
    }
"""

import logging
import json


class CustomLogger(logging.Logger):
    def makeRecord(
        self,
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        if extra:
            extra = {"fields": extra}
        record = super(CustomLogger, self).makeRecord(
            name,
            level,
            fn,
            lno,
            msg,
            args,
            exc_info,
            func=func,
            extra=extra,
            sinfo=sinfo,
        )
        return record


# Create a custom JSON log formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        fields = {"message": record.getMessage()}
        extra_fieds = record.__dict__.get("fields", {})
        fields.update(extra_fieds)
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "fields": fields,
            "module": record.module,
            "func_name": record.funcName,
            "path_name": record.pathname,
            "line_no": record.lineno,
        }
        return json.dumps(log_data)
