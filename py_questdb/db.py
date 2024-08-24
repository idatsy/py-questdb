"""Module for interacting with QuestDB."""

from types import TracebackType
from typing import Iterable, Type, overload, AsyncGenerator, Any, TypeVar

import aiohttp
import pandas as pd
import msgspec
import requests
from questdb.ingress import Sender

from py_questdb.db_types import QuestMessage, QuestDBResponse, TYPE_MAP, convert_types


T = TypeVar("T")


class QuestDB:
    """A class to interact with QuestDB for querying and writing data."""

    def __init__(self, host: str = "localhost", port: int = 9000, username: str | None = None, password: str | None = None):
        """
        Initialize a QuestDB connection.

        Args:
            host: The hostname of the QuestDB server. Defaults to "localhost".
            port: The port number for the HTTP interface. Defaults to 9000.
            username: Optional username for authentication.
            password: Optional password for authentication.

        Note:
            The TCP default port is 9009 and the HTTP default port is 9000.
        """
        self.url = f"http://{host}:{9000}"
        self.client = Sender("http", host, port, username=username, password=password, auto_flush=True)

        self.client.establish()  # unclear if this blocks, API keeps changing!
        self.session = None

    def write(self, *messages: QuestMessage) -> None:
        """
        Write multiple messages to QuestDB.

        Args:
            *messages: Variable number of QuestMessage objects to write.
        """
        for msg in messages:
            self.client.row(**msg.to_quest_db_format())
        self.client.flush()

    def write_iter(self, messages: Iterable[QuestMessage]) -> None:
        """
        Write an iterable of messages to QuestDB.

        Args:
            messages: An iterable of QuestMessage objects to write.
        """
        for msg in messages:
            self.client.row(**msg.to_quest_db_format())
        self.client.flush()

    def buffer_write(self, *messages: QuestMessage) -> None:
        """
        Buffer multiple messages for writing to QuestDB without flushing.

        Args:
            *messages: Variable number of QuestMessage objects to buffer.
        """
        for msg in messages:
            self.client.row(**msg.to_quest_db_format())

    def buffer_write_iter(self, messages: Iterable[QuestMessage]) -> None:
        """
        Buffer an iterable of messages for writing to QuestDB without flushing.

        Args:
            messages: An iterable of QuestMessage objects to buffer.
        """
        for msg in messages:
            self.client.row(**msg.to_quest_db_format())

    async def _query(self, query_string: str) -> bytes:
        """
        Perform an asynchronous query to QuestDB.

        Args:
            query_string: The SQL query string to execute.

        Returns:
            The raw bytes response from the server.
        """
        if self.session is None:
            self.session = aiohttp.ClientSession()

        async with self.session.get(f"{self.url}/exec", params={"query": query_string}) as response:
            response.raise_for_status()
            return await response.read()

    def _query_sync(self, query_string: str) -> bytes:
        """
        Perform a synchronous query to QuestDB.

        Args:
            query_string: The SQL query string to execute.

        Returns:
            The raw bytes response from the server.
        """
        response = requests.get(f"{self.url}/exec", {"query": query_string})
        response.raise_for_status()

        return response.content

    @staticmethod
    def parse_query_response(response: bytes) -> QuestDBResponse:
        """
        Parse the raw bytes response from QuestDB into a QuestDBResponse object.

        Args:
            response: The raw bytes response from the server.

        Returns:
            A QuestDBResponse object representing the parsed response.
        """
        return msgspec.json.decode(response, type=QuestDBResponse)

    @staticmethod
    def parse_and_yield_query_response(
        response: QuestDBResponse, into_type: Type[T] | None
    ) -> Iterable[T] | Iterable[dict]:
        """
        Parse and yield rows from a QuestDBResponse.

        Args:
            response: The QuestDBResponse object to parse.
            into_type: Optional type to cast each row into.

        Yields:
            Parsed rows as either dictionaries or instances of the specified type.
        """
        type_converter = [TYPE_MAP[col.type] for col in response.columns]

        for row in response.dataset:
            converted_row = {
                field.name: converter(value) if value is not None else None
                for field, converter, value
                in zip(response.columns, type_converter, row)
            }

            yield into_type(**converted_row) if into_type else converted_row

    @overload
    async def query(self, query_string: str) -> AsyncGenerator[dict[str, Any], None]: ...

    @overload
    async def query(self, query_string: str, into_type: Type[T]) -> AsyncGenerator[T, None]: ...

    async def query(self, query_string: str, into_type: Type[T] | None = None) -> AsyncGenerator[T, None]:
        """
        Perform an asynchronous query and yield the results.

        Args:
            query_string: The SQL query string to execute.
            into_type: Optional type to cast each row into.

        Yields:
            Parsed rows as either dictionaries or instances of the specified type.
        """
        data = self.parse_query_response(await self._query(query_string))

        for row in self.parse_and_yield_query_response(data, into_type):
            yield row

    @overload
    def query_sync(self, query_string: str) -> Iterable[dict[str, Any]]: ...

    @overload
    def query_sync(self, query_string: str, into_type: Type[T]) -> Iterable[T]: ...

    def query_sync(self, query_string: str, into_type: Type[T] | None = None) -> Iterable[T] | Iterable[dict]:
        """
        Perform a synchronous query and return an iterable of results.

        Args:
            query_string: The SQL query string to execute.
            into_type: Optional type to cast each row into.

        Returns:
            An iterable of parsed rows as either dictionaries or instances of the specified type.
        """
        data = self.parse_query_response(self._query_sync(query_string))

        return self.parse_and_yield_query_response(data, into_type)

    async def query_df(self, query_string: str) -> pd.DataFrame:
        """
        Perform an asynchronous query and return the results as a pandas DataFrame.

        Args:
            query_string: The SQL query string to execute.

        Returns:
            A pandas DataFrame containing the query results.
        """
        data = self.parse_query_response(await self._query(query_string))

        df = convert_types(pd.DataFrame(data.dataset, columns=[col.name for col in data.columns]), data.columns)

        # Set timestamp as index and sort descending
        timestamp_col = next((col.name for col in data.columns if col.type == "TIMESTAMP"), None)
        if timestamp_col:
            df.set_index(timestamp_col, inplace=True)
            df.sort_index(ascending=False, inplace=True)

        return df

    def query_df_sync(self, query_string: str) -> pd.DataFrame:
        """
        Perform a synchronous query and return the results as a pandas DataFrame.

        Args:
            query_string: The SQL query string to execute.

        Returns:
            A pandas DataFrame containing the query results.
        """
        data = self.parse_query_response(self._query_sync(query_string))

        df = convert_types(pd.DataFrame(data.dataset, columns=[col.name for col in data.columns]), data.columns)

        # Set timestamp as index and sort descending
        timestamp_col = next((col.name for col in data.columns if col.type == "TIMESTAMP"), None)
        if timestamp_col:
            df.set_index(timestamp_col, inplace=True)
            df.sort_index(ascending=False, inplace=True)

        return df

    async def close(self):
        """Close the QuestDB connection and any open sessions."""
        if self.session:
            await self.session.close()
        self.client.close()

    async def __aenter__(self) -> "QuestDB":
        """Async context manager entry."""
        self.session = aiohttp.ClientSession() if not self.session else self.session
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Async context manager exit."""
        if self.session:
            await self.close()
