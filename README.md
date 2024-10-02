# QuestDB Python Library

A robust, strongly-typed Python library for interacting with QuestDB, providing an easy-to-use interface for querying and writing data, with additional features such as easy integration with the built in python logging library automatically saving all logs, including tracebacks and stack traces to DB. Easy set-up with docker-compose and a grafana dashboard is included.

## Key Features

- Asynchronous and synchronous query support
- Strongly-typed data structures using `msgspec`
- Easy data insertion with automatic flushing
- Pandas DataFrame integration
- Seamless logging integration with Python's built-in logging system
- Docker Compose setup for QuestDB with Prometheus and Grafana monitoring
- Comprehensive test coverage
- Optional error handling callback

## Installation

Install the QuestDB Python library using pip:

```bash
pip install py-questdb
```

## Quick Start

```python
from py_questdb import QuestDB
from datetime import datetime

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

# Query with error handling
def error_handler(response: bytes):
    print(f"Error in query: {response.decode()}")

results = db.query_sync("SELECT * FROM non_existent_table", error_handler=error_handler)
```

### Writing Data

```python
from datetime import datetime
from py_questdb.db_types import QuestDBFields
import msgspec

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

### Logging Integration

The library provides seamless integration with Python's built-in logging system, allowing you to easily write logs directly to QuestDB:

```python
import logging
from py_questdb import QuestDB
from py_questdb.log_handler import QuestDBLogHandler

# Initialize QuestDB client
questdb_client = QuestDB()

# Create QuestDB log handler
handler = QuestDBLogHandler(questdb_client=questdb_client)

# Get logger and add handler
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Now all your logs will be written to QuestDB
logger.info("System started")
logger.warning("Low disk space")
logger.error("Connection failed", exc_info=True)

# You can log exceptions with full stack traces
try:
    1 / 0
except Exception as e:
    logger.exception("An error occurred")

# The logs are written to QuestDB in real-time, allowing for immediate analysis and monitoring
```

This integration allows you to leverage QuestDB's powerful querying capabilities for log analysis, making it easy to search, filter, and analyze your application logs.

## Docker Compose Setup

The project includes a Docker Compose configuration to run QuestDB along with Prometheus for metrics collection and Grafana for visualization. To use this setup:

1. Navigate to the `/etc` directory in the project.
2. Run the following command:

```bash
docker-compose up -d
```

This will start QuestDB, Prometheus, and Grafana containers. You can access:
- QuestDB Web Console: http://localhost:9000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (default credentials: admin/admin)

> A pre-configured Grafana dashboard for QuestDB metrics is included in the `questdb_dashboard.json` file, allowing for immediate visualization of your QuestDB instance's performance.

## Testing

Run the test suite using pytest:

```bash
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE).
