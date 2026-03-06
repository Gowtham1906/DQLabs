import logging
import time

from slack_sdk.signature import SignatureVerifier
from dotenv import dotenv_values
import subprocess

config = dotenv_values(".env")
signature_verifier = SignatureVerifier(config["SLACK_SIGNING_SECRET"])

from flask import Flask, request, make_response, jsonify
from slack_sdk import WebClient

client = WebClient(token=config["SLACK_BOT_TOKEN"])
app = Flask(__name__)


def log_subprocess_output(pipe):
    for line in iter(pipe.readline, b''):
        logging.info('got line from subprocess: %r', line)


@app.route("/slack/events/", methods=["POST"])
def slack_app():
    if True or request.values is not None and request.values["text"] is not None:
        should_build = False
        # command = request.values["text"] and request.values["text"].split(",") or []
        # deploy_app = command[0]
        deploy_app = "client"
        command=["client"]
        if deploy_app == "client" or len(command) > 1:
            if deploy_app == "airflow":
                tag_id = command[1]
                subprocess.Popen(["chmod", "u+x", "./scripts/updateairflow.sh"])
                subprocess.Popen(["sh", "scripts/updateairflow.sh", f"{tag_id}"])
            elif deploy_app == "client":
                subprocess.Popen(["chmod", "u+x", "scripts/buildclient.sh"])
                subprocess.Popen(["sh", "scripts/buildclient.sh"])
                return make_response("", 200)
                # subprocess.run("scripts/buildclient.sh", check=True, capture_output=True)
                # with subprocess.stdout:
                #     log_subprocess_output(subprocess.stdout)
            elif deploy_app == "server":
                tag_id = command[1]
                ## makemigration
                subprocess.Popen(["chmod", "u+x", "./scripts/djangomigration.sh"])
                subprocess.Popen(["sh", "scripts/djangomigration.sh"])
                time.sleep(60)
                subprocess.Popen(["chmod", "u+x", "./scripts/updateserver.sh"])
                subprocess.Popen(["sh", "scripts/updateserver.sh", f"{tag_id}"])
            return make_response(f"Build executed for {command[0]}. Please wait for 10 mins", 200)
        else:
            return make_response(
                f"Parameters in `deploy_app tag_id`. Possible values for deploy app is airflow/client/server", 400)
    return make_response("Parameters in `deploy_app version`. Possible values for deploy app is airflow/client/server",
                         400)


if __name__ == "__main__":
    app.run("0.0.0.0", 3000)
