import argparse
import os
import sys

import auth
from db import DuckDB
import sync


def prepare_data_dir(data_dir: str) -> None:
    """
    Ensure the data directory exists.

    Args:
        data_dir (str): The path where the data should be stored.

    Returns:
        None
    """
    os.makedirs(data_dir, exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["sync", "sync-message"], help="Command to run")
    parser.add_argument("--data-dir", required=True, help="Path where data is stored")
    parser.add_argument("--full-sync", action="store_true", help="Force a full sync of all messages")
    parser.add_argument("--message-id", help="The ID of the message to sync")

    args = parser.parse_args()

    prepare_data_dir(args.data_dir)
    credentials = auth.get_credentials(args.data_dir)

    # Initialize DuckDB (Singleton)
    db = DuckDB(args.data_dir)

    if args.command == "sync":
        total_synced = sync.fetch_all_messages(credentials, full_sync=args.full_sync)
        print(f"Total messages synced: {total_synced}")

    elif args.command == "sync-message":
        if not args.message_id:
            print("Please provide a message ID")
            sys.exit(1)
        sync.single_message(credentials, db, args.message_id)
