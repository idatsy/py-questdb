import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd

from py_questdb.db_types import QuestDBResponse, QuestDBColumn
from py_questdb.db import QuestDB


@pytest.fixture
def questdb_instance():
    with patch("py_questdb.db.Sender") as mock_sender:
        db = QuestDB(host="localhost", port=9000)
        db.client = mock_sender.return_value
        yield db


@pytest.fixture
def sample_response():
    return QuestDBResponse(
        query="SELECT * FROM test_table",
        columns=[
            QuestDBColumn(name="number", type="LONG"),
            QuestDBColumn(name="complex", type="DOUBLE"),
            QuestDBColumn(name="string", type="VARCHAR"),
            QuestDBColumn(name="tf", type="BOOLEAN"),
            QuestDBColumn(name="some_symbol", type="SYMBOL"),
            QuestDBColumn(name="timestamp", type="TIMESTAMP"),
        ],
        timestamp=1628097600000000,
        dataset=[
            (15, 3.14159, "hello", True, "AAPL", "2023-08-14T12:00:00.000000Z"),
        ],
        count=1,
    )


@pytest.mark.asyncio
async def test_write(questdb_instance):
    from tests.test_structs import TestMsg  # Import TestMsg here

    msg = TestMsg(number=15, complex=3.14159, string="hello", tf=True, some_symbol="AAPL")
    questdb_instance.write(msg)
    questdb_instance.client.row.assert_called_once()
    questdb_instance.client.flush.assert_called_once()


@pytest.mark.asyncio
async def test_query(questdb_instance, sample_response):
    questdb_instance._query = AsyncMock(return_value=b"{}")
    questdb_instance.parse_query_response = MagicMock(return_value=sample_response)

    results = [row async for row in questdb_instance.query("SELECT * FROM test_table")]

    assert len(results) == 1
    assert results[0]["number"] == 15
    assert results[0]["complex"] == pytest.approx(3.14159)
    assert results[0]["string"] == "hello"
    assert results[0]["tf"]
    assert results[0]["some_symbol"] == "AAPL"
    assert isinstance(results[0]["timestamp"], datetime)


@pytest.mark.asyncio
async def test_query_df(questdb_instance, sample_response):
    questdb_instance._query = AsyncMock(return_value=b"{}")
    questdb_instance.parse_query_response = MagicMock(return_value=sample_response)

    df = await questdb_instance.query_df("SELECT * FROM test_table")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.index.name == "timestamp"
    assert df.iloc[0]["number"] == 15
    assert df.iloc[0]["complex"] == pytest.approx(3.14159)
    assert df.iloc[0]["string"] == "hello"
    assert df.iloc[0]["tf"]
    assert df.iloc[0]["some_symbol"] == "AAPL"


def test_query_sync(questdb_instance, sample_response):
    questdb_instance._query_sync = MagicMock(return_value=b"{}")
    questdb_instance.parse_query_response = MagicMock(return_value=sample_response)

    results = list(questdb_instance.query_sync("SELECT * FROM test_table"))

    assert len(results) == 1
    assert results[0]["number"] == 15
    assert results[0]["complex"] == pytest.approx(3.14159)
    assert results[0]["string"] == "hello"
    assert results[0]["tf"]
    assert results[0]["some_symbol"] == "AAPL"
    assert isinstance(results[0]["timestamp"], datetime)


def test_query_df_sync(questdb_instance, sample_response):
    questdb_instance._query_sync = MagicMock(return_value=b"{}")
    questdb_instance.parse_query_response = MagicMock(return_value=sample_response)

    df = questdb_instance.query_df_sync("SELECT * FROM test_table")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.index.name == "timestamp"
    assert df.iloc[0]["number"] == 15
    assert df.iloc[0]["complex"] == pytest.approx(3.14159)
    assert df.iloc[0]["string"] == "hello"
    assert df.iloc[0]["tf"]
    assert df.iloc[0]["some_symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_context_manager(questdb_instance):
    questdb_instance.close = AsyncMock()

    async with questdb_instance as db:
        assert isinstance(db, QuestDB)

    questdb_instance.close.assert_awaited_once()
