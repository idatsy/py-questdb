import pytest
import logging
from unittest.mock import MagicMock, patch
from py_questdb import QuestDB
from py_questdb.log_handler import QuestDBLogHandler, InnerQuestDBLogHandler


def setup_logging(questdb_client: QuestDB):
    handler = QuestDBLogHandler(questdb_client)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
    return handler


def worker_process(worker_id):
    logger = logging.getLogger(f"worker_{worker_id}")
    if worker_id == 2:
        try:
            raise ValueError(f"This is an exception from worker {worker_id}")
        except ValueError as e:
            logger.error(f"This is an error message from worker {worker_id} {e}", exc_info=True, stack_info=True)
    logger.info(f"This is an info message from worker {worker_id}")


@pytest.fixture
def mock_questdb_client():
    mock_client = MagicMock()
    mock_questdb = MagicMock(spec=QuestDB)
    mock_questdb.client = mock_client
    return mock_questdb


@pytest.fixture
def questdb_log_handler(mock_questdb_client):
    return QuestDBLogHandler(mock_questdb_client)


def test_questdb_log_handler_init(mock_questdb_client):
    handler = QuestDBLogHandler(mock_questdb_client)
    assert isinstance(handler.inner_handler, InnerQuestDBLogHandler)
    assert handler.listener is not None


def test_questdb_log_handler_prepare(questdb_log_handler):
    record = logging.LogRecord(
        name="test_logger", level=logging.INFO, pathname="test_path", lineno=42, msg="Test message", args=(), exc_info=None
    )
    prepared_record = questdb_log_handler.prepare(record)
    assert prepared_record == record


def test_questdb_log_handler_close(questdb_log_handler):
    with patch.object(questdb_log_handler.listener, "stop") as mock_stop:
        questdb_log_handler.close()
        mock_stop.assert_called_once()
    assert questdb_log_handler.listener is None


def test_inner_questdb_log_handler_emit(mock_questdb_client):
    handler = InnerQuestDBLogHandler(mock_questdb_client)
    record = logging.LogRecord(
        name="test_logger", level=logging.INFO, pathname="test_path", lineno=42, msg="Test message", args=(), exc_info=None
    )
    handler.emit(record)
    mock_questdb_client.client.row.assert_called_once()


def test_setup_logging(mock_questdb_client):
    with patch("logging.getLogger") as mock_get_logger:
        mock_root_logger = MagicMock()
        mock_get_logger.return_value = mock_root_logger
        handler = setup_logging(mock_questdb_client)
        assert isinstance(handler, QuestDBLogHandler)
        mock_root_logger.setLevel.assert_called_with(logging.DEBUG)
        mock_root_logger.addHandler.assert_called_with(handler)


@patch("logging.getLogger")
def test_worker_process(mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Test normal case
    worker_process(1)
    mock_logger.info.assert_called_with("This is an info message from worker 1")

    # Test exception case
    worker_process(2)
    mock_logger.error.assert_called()
    assert "This is an error message from worker 2" in mock_logger.error.call_args[0][0]
    assert mock_logger.error.call_args[1]["exc_info"] is True
    assert mock_logger.error.call_args[1]["stack_info"] is True
