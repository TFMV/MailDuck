import json
import logging
import time
import duckdb
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from message import Message
from auth import get_credentials

MAX_RESULTS = 500
BATCH_SIZE = 50  # Fetch emails in batches

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DuckDB database connection
db_path = "mailduck.db"
conn = duckdb.connect(db_path)
conn.execute("""
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

def get_labels(service) -> dict:
    """
    Retrieves all Gmail labels for the authenticated user.
    """
    labels = {}
    try:
        response = service.users().labels().list(userId="me").execute()
        for label in response.get("labels", []):
            labels[label["id"]] = label["name"]
    except HttpError as e:
        logger.error(f"Failed to fetch labels: {e}")
    return labels


def fetch_all_messages(credentials, full_sync=False) -> int:
    """
    Fetches all messages from Gmail API and stores them in DuckDB.

    Args:
        credentials (object): Authenticated credentials.
        full_sync (bool): Whether to perform a full sync.

    Returns:
        int: Number of messages fetched.
    """

    query = []
    if not full_sync:
        last_timestamp = get_last_indexed_timestamp()
        if last_timestamp:
            query.append(f"after:{int(last_timestamp.timestamp())}")

    service = build("gmail", "v1", credentials=credentials)
    labels = get_labels(service)

    page_token = None
    total_messages = 0
    batch = []

    while True:
        try:
            response = (
                service.users()
                .messages()
                .list(userId="me", maxResults=MAX_RESULTS, pageToken=page_token, q=" ".join(query))
                .execute()
            )

            messages = response.get("messages", [])
            total_messages += len(messages)

            if not messages:
                break

            # Fetch message details in batches
            for i in range(0, len(messages), BATCH_SIZE):
                batch_ids = [m["id"] for m in messages[i : i + BATCH_SIZE]]
                batch.extend(fetch_message_batch(service, batch_ids, labels))

                if len(batch) >= 100:  # Insert in chunks of 100
                    save_to_duckdb(batch)
                    batch.clear()

            if "nextPageToken" in response:
                page_token = response["nextPageToken"]
            else:
                break

        except HttpError as e:
            logger.error(f"Gmail API error: {e}")
            time.sleep(5)  # Retry delay
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    if batch:
        save_to_duckdb(batch)

    return total_messages


def fetch_message_batch(service, message_ids, labels):
    """
    Fetches a batch of messages from Gmail API using individual requests.
    
    Args:
        service: Gmail API service instance.
        message_ids: List of message IDs to fetch.
        labels: Dictionary mapping label IDs to label names.
    
    Returns:
        list[dict]: Parsed message records.
    """
    messages = []
    
    for message_id in message_ids:
        try:
            msg = service.users().messages().get(userId="me", id=message_id, format="full").execute()
            messages.append(Message.from_raw(msg, labels).__dict__)
        except Exception as e:
            logger.error(f"Failed to fetch message {message_id}: {e}")
    
    return messages


def save_to_duckdb(messages):
    """
    Inserts messages into DuckDB in bulk.

    Args:
        messages (list[dict]): List of messages to insert.
    """
    if not messages:
        return

    df = pd.DataFrame(messages)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["last_indexed"] = pd.Timestamp.now(tz="UTC")

    conn.execute("INSERT INTO messages SELECT * FROM df")
    logger.info(f"Inserted {len(messages)} messages into DuckDB.")


def get_last_indexed_timestamp():
    """
    Retrieves the latest indexed message timestamp from DuckDB.

    Returns:
        datetime.datetime or None: The last indexed timestamp.
    """
    result = conn.execute("SELECT MAX(timestamp) FROM messages").fetchone()
    return result[0] if result[0] else None


if __name__ == "__main__":
    credentials = get_credentials(".")
    total_synced = fetch_all_messages(credentials, full_sync=False)
    logger.info(f"Total messages synced: {total_synced}")
