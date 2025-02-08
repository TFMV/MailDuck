import json
import os
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Define required OAuth scopes
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
DEFAULT_CREDENTIALS_FILE = "credentials.json"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_credentials(data_dir: str, credentials_path: str = DEFAULT_CREDENTIALS_FILE) -> Credentials:
    """
    Retrieves authentication credentials for the specified data_dir.
    If valid credentials exist, they are loaded; otherwise, the OAuth flow runs.

    Args:
        data_dir (str): Directory where credentials are stored.
        credentials_path (str): Path to OAuth2 client credentials.

    Returns:
        google.oauth2.credentials.Credentials: The authentication credentials.
    """
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"OAuth credentials file '{credentials_path}' not found.")

    token_path = os.path.join(data_dir, "token.json")

    credentials = None

    # Check if token exists and load it
    if os.path.exists(token_path):
        with open(token_path, "r") as token_file:
            credentials = Credentials.from_authorized_user_info(json.load(token_file))

    # If credentials are missing or expired, refresh or re-authenticate
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            logger.info("Refreshing expired credentials...")
            credentials.refresh(Request())
        else:
            logger.info("Running OAuth flow for authentication...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            credentials = flow.run_local_server(port=0)

            # Save new credentials
            with open(token_path, "w") as token_file:
                token_file.write(credentials.to_json())

    logger.info("Authentication successful.")
    return credentials
