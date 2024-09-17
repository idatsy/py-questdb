# QuestDB Python Library

A robust, strongly-typed Python library for interacting with QuestDB, providing an easy-to-use interface for querying, writing data, and managing logs.

## Features

- Asynchronous and synchronous query support
- Strongly-typed data structures
- Easy data insertion with automatic flushing
- Pandas DataFrame integration
- Built-in logging handler for QuestDB
- Comprehensive test coverage

## Installation

Install the QuestDB Python library using pip:

```bash
pip install py-questdb
```

## Quick Start

```python
from py_questdb import QuestDB

# Initialize QuestDB client
db = QuestDB(host="localhost", port=9000)

# Write data
db.write({
    "table_name": "my_table",
    "symbols": {"symbol_column": "AAPL"},
    "columns": {"number_column": 42, "string_column": "Hello, QuestDB!"},
    "at": datetime.now()
})

# Query data (synchronous)
for row in db.query_sync("SELECT * FROM my_table"):
    print(row)

# Query data (asynchronous)
async for row in db.query("SELECT * FROM my_table"):
    print(row)

# Query data as Pandas DataFrame
df = db.query_df_sync("SELECT * FROM my_table")
print(df)
```

## Detailed Usage

### Initialization

```python
from py_questdb import QuestDB

# Default connection
db = QuestDB()

# Custom connection
db = QuestDB(host="questdb.example.com", port=9000, username="user", password="pass")
```

### Writing Data

```python
from datetime import datetime
from py_questdb.db_types import QuestDBFields

# Single write with auto-flush
db.write({
    "table_name": "sensors",
    "symbols": {"location": "NYC"},
    "columns": {"temperature": 25.5, "humidity": 60},
    "at": datetime.now()
})

# Buffer multiple writes
db.buffer_write({
    "table_name": "sensors",
    "symbols": {"location": "LA"},
    "columns": {"temperature": 28.0, "humidity": 55},
    "at": datetime.now()
})

# Manually flush buffered writes
db.flush()

# Write using custom message class
class SensorData(msgspec.Struct):
    location: str
    temperature: float
    humidity: int
    timestamp: datetime

    def to_quest_db_format(self) -> QuestDBFields:
        return {
            "table_name": "sensors",
            "symbols": {"location": self.location},
            "columns": {"temperature": self.temperature, "humidity": self.humidity},
            "at": self.timestamp
        }

sensor_data = SensorData(location="SF", temperature=22.5, humidity=65, timestamp=datetime.now())
db.write(sensor_data)
```

### Querying Data

```python
# Synchronous query
for row in db.query_sync("SELECT * FROM sensors WHERE location = 'NYC'"):
    print(f"Temperature: {row['temperature']}, Humidity: {row['humidity']}")

# Asynchronous query
async for row in db.query("SELECT * FROM sensors WHERE location = 'LA'"):
    print(f"Temperature: {row['temperature']}, Humidity: {row['humidity']}")

# Query into Pandas DataFrame
df = db.query_df_sync("SELECT * FROM sensors")
print(df.describe())

# Query with type hinting
class SensorReading(msgspec.Struct):
    location: str
    temperature: float
    humidity: int

async for reading in db.query("SELECT * FROM sensors", into_type=SensorReading):
    print(f"Location: {reading.location}, Temp: {reading.temperature}°C, Humidity: {reading.humidity}%")
```

### Logging

```python
import logging
from py_questdb import QuestDB
from py_questdb.log_handler import QuestDBLogHandler

# Set up QuestDB logging
questdb_client = QuestDB()
handler = QuestDBLogHandler(questdb_client=questdb_client)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Log messages
logger.info("System started")
logger.warning("Low disk space")
logger.error("Connection failed", exc_info=True)
```

## Testing

Run the test suite using pytest:

```bash
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE).