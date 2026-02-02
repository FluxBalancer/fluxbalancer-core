import json
import logging


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if isinstance(record.msg, dict):
            payload = {
                "time": self.formatTime(record),
                **record.msg,
            }
            return json.dumps(payload, ensure_ascii=False)
        return super().format(record)


def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)
