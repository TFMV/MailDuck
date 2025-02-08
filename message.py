import base64
import logging
from email.utils import parseaddr, parsedate_to_datetime

from bs4 import BeautifulSoup


class Message:
    def __init__(self):
        self.id = None
        self.thread_id = None
        self.sender = {}
        self.recipients = {"to": [], "cc": [], "bcc": []}  # Ensures default structure
        self.labels = []
        self.subject = None
        self.body = ""
        self.size = 0
        self.timestamp = None
        self.is_read = False
        self.is_outgoing = False

    @classmethod
    def from_raw(cls, raw: dict, labels: dict):
        """
        Create a Message object from a raw Gmail API message.

        Args:
            raw (dict): The raw message.
            labels (dict): A dictionary mapping Gmail label IDs to label names.

        Returns:
            Message: The parsed message.
        """
        msg = cls()
        msg.parse(raw, labels)
        return msg

    def parse_addresses(self, addresses: str) -> list:
        """
        Parse a list of email addresses.

        Args:
            addresses (str): Comma-separated email addresses.

        Returns:
            list[dict]: Parsed addresses as dictionaries with 'email' and 'name'.
        """
        parsed_addresses = []
        if addresses:
            for address in addresses.split(","):
                name, email = parseaddr(address)
                if email:
                    parsed_addresses.append({"email": email.lower(), "name": name})
        return parsed_addresses

    def decode_body(self, part) -> str:
        """
        Recursively decodes the body of a Gmail message.

        Args:
            part (dict): A message part.

        Returns:
            str: The decoded message body.
        """
        if "body" in part and "data" in part["body"]:
            try:
                return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8")
            except Exception as e:
                logging.warning(f"Failed to decode message body: {e}")
                return ""
        elif "parts" in part:
            for subpart in part["parts"]:
                decoded_body = self.decode_body(subpart)
                if decoded_body:
                    return decoded_body
        return ""

    def html2text(self, html: str) -> str:
        """
        Convert HTML to plain text.

        Args:
            html (str): The HTML content.

        Returns:
            str: Extracted plain text.
        """
        soup = BeautifulSoup(html, features="html.parser")
        return soup.get_text()

    def parse(self, msg: dict, labels: dict) -> None:
        """
        Parses a raw Gmail API message.

        Args:
            msg (dict): The message payload from Gmail API.
            labels (dict): A dictionary mapping Gmail label IDs to label names.

        Returns:
            None
        """
        self.id = msg.get("id")
        self.thread_id = msg.get("threadId")
        self.size = msg.get("sizeEstimate", 0)

        # Parse headers
        headers = {header["name"].lower(): header["value"] for header in msg.get("payload", {}).get("headers", [])}

        self.sender = {"name": "", "email": ""}
        if "from" in headers:
            name, email = parseaddr(headers["from"])
            self.sender = {"name": name, "email": email}

        self.recipients["to"] = self.parse_addresses(headers.get("to", ""))
        self.recipients["cc"] = self.parse_addresses(headers.get("cc", ""))
        self.recipients["bcc"] = self.parse_addresses(headers.get("bcc", ""))

        self.subject = headers.get("subject", "")

        # Handle timestamp safely
        date_header = headers.get("date")
        if date_header:
            try:
                self.timestamp = parsedate_to_datetime(date_header)
            except Exception as e:
                logging.warning(f"Failed to parse email date: {date_header} - {e}")
                self.timestamp = None

        # Parse labels
        if "labelIds" in msg:
            self.labels = [labels.get(l, l) for l in msg["labelIds"]]
            self.is_read = "UNREAD" not in msg["labelIds"]
            self.is_outgoing = "SENT" in msg["labelIds"]

        # Extract body
        payload = msg.get("payload", {})
        self.body = self.extract_body(payload)

    def extract_body(self, payload: dict) -> str:
        """
        Extracts the message body from a Gmail payload.

        Args:
            payload (dict): The Gmail message payload.

        Returns:
            str: Extracted plain-text message body.
        """
        if not payload:
            return ""

        # Try direct extraction
        if "body" in payload and "data" in payload["body"]:
            try:
                return self.html2text(base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8"))
            except Exception as e:
                logging.warning(f"Error decoding email body: {e}")
                return ""

        # Check parts
        if "parts" in payload:
            for part in payload["parts"]:
                mime_type = part.get("mimeType", "")
                if mime_type in ["text/plain", "text/html", "multipart/related", "multipart/alternative"]:
                    extracted_body = self.decode_body(part)
                    if extracted_body:
                        return self.html2text(extracted_body)

        return ""
