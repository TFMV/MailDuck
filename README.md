# MailDuck 🦆📨

Extract Gmail messages into DuckDB for easy querying & analysis

## Installation

1. Clone this repository: `git clone https://github.com/marcboeker/gmail-to-sqlite.git`.
2. Install the requirements: `pip install -r requirements.txt`
3. Create a Google Cloud project [here](https://console.cloud.google.com/projectcreate).
4. Open [Gmail in API & Services](https://console.cloud.google.com/apis/library/gmail.googleapis.com) and activate the Gmail API.
5. Open the [OAuth consent screen](https://console.cloud.google.com/apis/credentials/consent) and create a new consent screen. You only need to provide a name and contact data.
6. Next open [Create OAuth client ID](https://console.cloud.google.com/apis/credentials/oauthclient) and create credentials for a `Desktop app`. Download the credentials file and save it under `credentials.json` in the root of this repository.

Here is a detailed guide on how to create the credentials: [https://developers.google.com/gmail/api/quickstart/python#set_up_your_environment](https://developers.google.com/gmail/api/quickstart/python#set_up_your_environment).

## Usage

### Sync all emails

1. Run the script: `python main.py sync --data-dir path/to/your/data` where `--<data-dir>` is the path where all data is stored. This creates a DuckDB database in `<data-dir>/messages.db` and stores the user credentials under `<data-dir>/credentials.json`.
2. After the script has finished, you can query the database using, for example, the `duckdb` command line tool: `duckdb <data-dir>/messages.db`.
3. You can run the script again to sync all new messages. Provide `--full-sync` to force a full sync. However, this will only update the read status, the labels, and the last indexed timestamp for existing messages.

### Sync a single message

`python main.py sync-message --data-dir path/to/your/data --message-id <message-id>`

## Commandline parameters

- `--data-dir`: Path to the directory where the data is stored.
- `--full-sync`: Force a full sync of all messages.
- `--message-id`: The ID of the message to sync.


## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Gmail to SQLite](https://github.com/marcboeker/gmail-to-sqlite)
- [DuckDB](https://duckdb.org/)
- [Pandas](https://pandas.pydata.org/)
- [Google Gmail API](https://developers.google.com/gmail/api)
