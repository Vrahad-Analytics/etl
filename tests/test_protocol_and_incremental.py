import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app import create_platform
from src.protocol import (
    AirbyteMessage,
    MessageType,
    Status,
    make_log,
    make_record,
    make_state,
)


class ProtocolAndIncrementalTests(unittest.TestCase):
    def test_protocol_message_serialization(self):
        record_msg = make_record("users", {"id": 1, "name": "Alice"})
        data = record_msg.to_dict()
        self.assertEqual(data["type"], "RECORD")
        self.assertEqual(data["record"]["stream"], "users")
        self.assertEqual(data["record"]["data"]["name"], "Alice")

        # Roundtrip JSON
        json_str = record_msg.to_json()
        deserialized = AirbyteMessage.from_dict(json.loads(json_str))
        self.assertEqual(deserialized.type, MessageType.RECORD)
        self.assertEqual(deserialized.record.stream, "users")
        self.assertEqual(deserialized.record.data["id"], 1)

        # State message
        state_msg = make_state({"cursor_value": 42}, stream="users")
        self.assertEqual(state_msg.type, MessageType.STATE)
        self.assertEqual(state_msg.state.data["cursor_value"], 42)

        # Log message
        log_msg = make_log("WARN", "Disk getting full")
        self.assertEqual(log_msg.type, MessageType.LOG)
        self.assertEqual(log_msg.log.message, "Disk getting full")

    def test_incremental_sync_with_cursor_and_state(self):
        with TemporaryDirectory() as temp_dir:
            platform = create_platform(temp_dir)

            # 1. Setup source data file with 2 initial rows
            csv_path = platform.write_text_file(
                "uploads/events.csv",
                "id,event,timestamp\n1,login,100\n2,click,200\n"
            )

            pipeline = platform.create_pipeline(
                {
                    "name": "incremental-events",
                    "sync_mode": "incremental",
                    "cursor_field": "timestamp",
                    "primary_key": "id",
                    "source": {"type": "csv_file", "config": {"path": str(csv_path), "has_header": True}},
                    "transformations": [],
                    "destination": {"type": "sqlite_file", "config": {"path": "warehouse/events.db", "table": "events"}},
                }
            )

            # First run: should sync 2 records and save checkpoint timestamp=200
            platform.enqueue_pipeline_run(pipeline["id"])
            job1 = platform.process_next_job()
            self.assertEqual(job1["status"], "succeeded")
            self.assertEqual(job1["record_count"], 2)

            state1 = platform.get_pipeline_state(pipeline["id"])
            self.assertIsNotNone(state1)
            self.assertEqual(str(state1["cursor_value"]), "200")

            # Check DB rows
            db_path = Path(temp_dir) / "warehouse" / "events.db"
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute("SELECT id, event, timestamp FROM events ORDER BY id").fetchall()
            self.assertEqual(len(rows), 2)

            # 2. Append new row (timestamp=300) and run 2nd sync
            platform.write_text_file(
                "uploads/events.csv",
                "id,event,timestamp\n1,login,100\n2,click,200\n3,purchase,300\n"
            )

            platform.enqueue_pipeline_run(pipeline["id"])
            job2 = platform.process_next_job()
            self.assertEqual(job2["status"], "succeeded")
            # Only 1 new record should be read and loaded!
            self.assertEqual(job2["record_count"], 1)

            state2 = platform.get_pipeline_state(pipeline["id"])
            self.assertEqual(str(state2["cursor_value"]), "300")

            # Check DB rows has all 3 records now
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute("SELECT id, event, timestamp FROM events ORDER BY id").fetchall()
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[2][1], "purchase")

            # 3. Test Reset state: next sync will re-read everything from scratch
            reset_result = platform.reset_pipeline_state(pipeline["id"])
            self.assertTrue(reset_result["ok"])
            self.assertIsNone(platform.get_pipeline_state(pipeline["id"]))


if __name__ == "__main__":
    unittest.main()
