import os
import duckdb
import pandas as pd
from datetime import datetime

DB_FILE = "messages.duckdb"


class DuckDB:
    """
    Singleton class for managing DuckDB connection.
    """

    def __init__(self, data_dir: str):
        self.db_path = os.path.join(data_dir, DB_FILE)
        self.conn = duckdb.connect(self.db_path)
        self.init_db()

    def init_db(self):
        """
        Initializes the database schema if it does not exist.
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                thread_id TEXT,
                sender JSON,
                recipients JSON,
                labels JSON,
                subject TEXT,
                body TEXT,
                size INTEGER,
                timestamp TIMESTAMP,
                is_read BOOLEAN,
                is_outgoing BOOLEAN,
                last_indexed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def bulk_insert_messages(self, messages: list[dict]):
        """
        Inserts a batch of messages into DuckDB.

        Args:
            messages (list[dict]): List of messages to insert.
        """
        if not messages:
            return

        df = pd.DataFrame(messages)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["last_indexed"] = pd.Timestamp.now()

        self.conn.execute("INSERT INTO messages SELECT * FROM df")

    def upsert_messages(self, messages: list[dict]):
        """
        Inserts new messages and updates existing ones in a single batch operation.

        Args:
            messages (list[dict]): List of messages to insert or update.
        """
        if not messages:
            return

        df = pd.DataFrame(messages)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["last_indexed"] = pd.Timestamp.now()

        self.conn.execute("""
            CREATE TEMP TABLE new_messages AS SELECT * FROM df
        """)

        self.conn.execute("""
            MERGE INTO messages AS target
            USING new_messages AS source
            ON target.message_id = source.message_id
            WHEN MATCHED THEN
                UPDATE SET
                    is_read = source.is_read,
                    last_indexed = source.last_indexed,
                    labels = source.labels
            WHEN NOT MATCHED THEN
                INSERT *;
        """)

        self.conn.execute("DROP TABLE new_messages")

    def get_last_indexed_timestamp(self):
        """
        Retrieves the latest indexed message timestamp.

        Returns:
            datetime.datetime or None
        """
        result = self.conn.execute("SELECT MAX(timestamp) FROM messages").fetchone()
        return result[0] if result[0] else None

    def get_first_indexed_timestamp(self):
        """
        Retrieves the earliest indexed message timestamp.

        Returns:
            datetime.datetime or None
        """
        result = self.conn.execute("SELECT MIN(timestamp) FROM messages").fetchone()
        return result[0] if result[0] else None

    def list_messages(self, limit: int = 10):
        """
        Retrieve the latest email messages.

        Args:
            limit (int): The maximum number of messages to return.

        Returns:
            list[dict]: A list of messages as dictionaries.
        """
        df = self.conn.execute(
            f"SELECT * FROM messages ORDER BY timestamp DESC LIMIT {limit}"
        ).fetchdf()
        return df.to_dict(orient="records")
