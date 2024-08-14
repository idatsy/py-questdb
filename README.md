# QuestDB Python Client

This is an open-source Python library for interacting with QuestDB, a high-performance, open-source SQL database designed to process time series data. This client provides an easy-to-use interface for querying and writing data to QuestDB from Python applications.

## Features

- Asynchronous and synchronous query execution
- Write single or multiple messages to QuestDB
- Buffer writes for improved performance
- Convert query results to Pandas DataFrames
- Type conversion and parsing of query results
- Support for both HTTP and TCP interfaces

## Installation

You can install the QuestDB Python Client using pip:

```bash
pip install py-questdb
```

## Quick Start

Here's a simple example to get you started:

```python
import asyncio
from questdb_python_client import QuestDB

async def main():
    # Connect to QuestDB
    db = QuestDB(host="localhost", port=9000)

    # Write some data
    await db.write(
        table_name="sensors",
        symbols={"location": "room1"},
        columns={"temperature": 22.5, "humidity": 60},
        at=datetime.now()
    )

    # Query data
    async for row in db.query("SELECT * FROM sensors ORDER BY timestamp DESC LIMIT 10"):
        print(row)

    # Query data as a DataFrame
    df = await db.query_df("SELECT * FROM sensors WHERE location = 'room1'")
    print(df)

    await db.close()

asyncio.run(main())
```

## Usage

### Connecting to QuestDB

```python
from questdb_python_client import QuestDB

# For HTTP interface
db = QuestDB(host="localhost", port=9000)

# For TCP interface
db = QuestDB(host="localhost", port=9009)
```

### Writing Data

```python
# Write a single message
db.write(
    table_name="sensors",
    symbols={"location": "room1"},
    columns={"temperature": 22.5, "humidity": 60},
    at=datetime.now()
)

# Write multiple messages
messages = [
    QuestMessage(table_name="sensors", symbols={"location": "room1"}, columns={"temperature": 22.5}, at=datetime.now()),
    QuestMessage(table_name="sensors", symbols={"location": "room2"}, columns={"temperature": 23.1}, at=datetime.now())
]
db.write_iter(messages)
```

### Querying Data

```python
# Asynchronous query
async for row in db.query("SELECT * FROM sensors WHERE location = 'room1'"):
    print(row)

# Synchronous query
for row in db.query_sync("SELECT * FROM sensors WHERE location = 'room1'"):
    print(row)

# Query as DataFrame
df = await db.query_df("SELECT * FROM sensors WHERE location = 'room1'")
print(df)
```

## License

This project is licensed under the [MIT License](LICENSE).