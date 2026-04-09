import unittest

from app.agent.utils import detect_database_mismatch
from app.schemas.connection import ColumnInfo, TableSchema


class RetrievalTimeoutGuardTest(unittest.TestCase):
    def test_timeout_error_does_not_trigger_database_mismatch(self) -> None:
        tables = [
            TableSchema(
                name="t_user",
                columns=[ColumnInfo(name="id", type="BIGINT", nullable=False)],
                comment="???",
            )
        ]
        detected, message = detect_database_mismatch(
            question="???????????",
            schema_tables=tables,
            sql="SELECT user_id, COUNT(*) FROM user_activities",
            error_message="retrieval operation timed out after 8.000 seconds",
        )
        self.assertFalse(detected)
        self.assertIsNone(message)


if __name__ == "__main__":
    unittest.main()
