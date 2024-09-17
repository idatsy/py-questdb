"""
USAGE:
    questdb_client = QuestDB()
    handler = QuestDBLogHandler(questdb_client=questdb_client)
    logger.addHandler(handler)
"""

import logging
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from datetime import datetime
from typing import Optional
from py_questdb import QuestDB


class QuestDBLogHandler(QueueHandler):
    def __init__(self, questdb_client: QuestDB, table_name: str = "_logs", level: Optional[str] = None):
        self.queue = Queue()
        super().__init__(self.queue)

        if level:
            self.setLevel(level)

        self.inner_handler = InnerQuestDBLogHandler(questdb_client=questdb_client, table_name=table_name)

        self.listener = QueueListener(self.queue, self.inner_handler, respect_handler_level=True)
        self.listener.start()

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        if record.exc_info:
            record.exc_info = logging._defaultFormatter.formatException(record.exc_info)  # noqa
        return record

    def close(self) -> None:
        try:
            if self.listener:
                self.listener.stop()
                self.listener = None
        except Exception as e:
            print(f"Error stopping QueueListener: {e}")
        finally:
            self.inner_handler.questdb_client.flush()
            super().close()

    def __del__(self):
        self.close()


class InnerQuestDBLogHandler(logging.Handler):
    def __init__(self, questdb_client: QuestDB, table_name: str = "_logs"):
        super().__init__()
        self.questdb_client = questdb_client
        self.table_name = table_name

    def emit(self, record: logging.LogRecord):
        try:
            print(f"Received {record=}")
            log_message = self.format_record(record)
            print(f"Writing {log_message=}")
            self.questdb_client.client.row(**log_message)
        except Exception as e:
            print(f"Failed to write log to QuestDB: {e}")

    def format_record(self, record: logging.LogRecord) -> dict:
        print(record.stack_info)
        return {
            "table_name": self.table_name,
            "symbols": {
                "level": record.levelname,
                "logger": record.name,
                "filename": record.filename,
                "funcName": record.funcName,
                "module": record.module,
                "processName": record.processName,
                "threadName": record.threadName,
                "pathname": record.pathname,
            },
            "columns": {
                "message": record.getMessage(),
                "lineno": record.lineno,
                "process": record.process,
                "thread": record.thread,
                "exc_info": record.exc_info,
                "stack_info": record.stack_info,
            },
            "at": datetime.fromtimestamp(record.created),
        }
