import logging

from slack_sdk.signature import SignatureVerifier
from dotenv import dotenv_values
import sys
from slack_sdk import WebClient

config = dotenv_values(".env")
signature_verifier = SignatureVerifier(config["SLACK_SIGNING_SECRET"])

client = WebClient(token=config["SLACK_BOT_TOKEN"])


def notify_slack_channel(channel_id, notification_str):
    client.chat_postMessage(channel=channel_id, text=notification_str)
    return None


notify_slack_channel(sys.argv[0], sys.argv[1])
