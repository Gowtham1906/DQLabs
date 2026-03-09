import os
import ssl
import subprocess
# Use the package we installed
import slack
from slack_bolt import App
from dotenv import dotenv_values

config = dotenv_values(".env")

# client = slack.WebClient(token=config["SLACK_BOT_TOKEN"], ssl=ssl_context)

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

sc = slack.WebClient(config["SLACK_BOT_TOKEN"],ssl=ssl_context)

# Initializes your app with your bot token and signing secret
# app = App(
#     token=config["SLACK_BOT_TOKEN"],
#     signing_secret=config["SLACK_SIGNING_SECRET"],
#     ssl_check_enabled=False)


@sc.command("/deploy_id")
def print_command(ack, response, command):
    try:
        subprocess.Popen(["chmod", "u+x", "./scripts/updateairflow.sh"])
        subprocess.run('./scripts/updateairflow.sh ', check=True, capture_output=True)
        ack()
    except subprocess.CalledProcessError as e:
        print(f"Unexpected error occured {e}")


if __name__ == "__main__":
    app.start(port=3000)
