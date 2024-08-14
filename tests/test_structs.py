from datetime import datetime
import msgspec
from py_questdb.db_types import QuestDBFields


class TestMsg(msgspec.Struct):
    number: int = 0
    complex: float = 0.0
    string: str = ""
    tf: bool = False
    some_symbol: str = ""
    timestamp: datetime | None = None

    def to_quest_db_format(self) -> QuestDBFields:
        print("to_quest_db_format called")
        return {
            "table_name": "test_table",
            "symbols": {"some_symbol": self.some_symbol},
            "columns": {"number": self.number, "complex": self.complex, "string": self.string, "tf": self.tf},
            "at": self.timestamp or datetime.now(),
        }

    def __hash__(self):
        return hash((self.number, self.complex, self.string, self.tf, self.some_symbol, self.timestamp))
