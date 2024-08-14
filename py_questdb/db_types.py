"""Types and utility functions for interacting with QuestDB."""

import datetime
from typing import TypedDict, Union, Protocol

import msgspec
import pandas as pd
from questdb.ingress import TimestampMicros


QuestDBField = Union[None, bool, int, float, str, TimestampMicros, datetime.datetime]

TYPE_MAP = {
    "SYMBOL": str,
    "BOOLEAN": bool,
    "LONG": int,
    "DOUBLE": float,
    "VARCHAR": str,
    "TIMESTAMP": datetime.datetime.fromisoformat,
}


class QuestDBColumn(msgspec.Struct):
    """Represents a column in a QuestDB table."""

    name: str
    type: str


def convert_types(df: pd.DataFrame, columns: list[QuestDBColumn]) -> pd.DataFrame:
    """
    Convert DataFrame column types based on QuestDB column definitions.

    Args:
        df: Input DataFrame.
        columns: List of QuestDBColumn objects defining column types.

    Returns:
        DataFrame with converted column types.
    """
    for col in columns:
        if col.type == "TIMESTAMP":
            df[col.name] = pd.to_datetime(df[col.name])
        elif col.type in ["BOOLEAN", "BYTE", "SHORT", "INT", "LONG"]:
            df[col.name] = pd.to_numeric(df[col.name], downcast="integer")
        elif col.type in ["FLOAT", "DOUBLE"]:
            df[col.name] = pd.to_numeric(df[col.name], downcast="float")
        elif col.type == "STRING":
            df[col.name] = df[col.name].astype(str)

    return df


class QuestDBResponse(msgspec.Struct):
    """Represents a response from a QuestDB query."""

    query: str
    columns: list[QuestDBColumn]
    timestamp: int
    dataset: list[tuple]
    count: int


class QuestDBFields(TypedDict):
    """Represents the fields required for inserting data into QuestDB."""

    table_name: str
    symbols: dict[str, str | None]
    columns: dict[str, QuestDBField]
    at: datetime.datetime  # Nanosecond timestamp used for indexing in the DB. Required!


class QuestMessage(Protocol):
    """Protocol for objects that can be published to QuestDB."""

    def to_quest_db_format(self) -> QuestDBFields:
        """
        Convert the object to a format suitable for insertion into QuestDB.

        Returns:
            A QuestDBFields object representing the data to be inserted.
        """
        ...
