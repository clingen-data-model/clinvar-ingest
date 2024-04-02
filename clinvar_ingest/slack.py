import logging

import requests

import clinvar_ingest.config

_logger = logging.getLogger("clinvar_ingest")


def send_slack_message(message: str) -> None:
    app_env = clinvar_ingest.config.get_env()
    if not app_env.slack_token or not app_env.slack_channel:
        _logger.warning(
            "Both a slack channel and auth token are required. Slack messaging is disabled."
        )
    else:
        slack_url = "https://slack.com/api/chat.postMessage"
        data = {"text": message, "channel": app_env.slack_channel}
        resp = requests.post(
            slack_url,
            json=data,
            headers={"Authorization": f"Bearer {app_env.slack_token}"},
        )
        if not resp.json()["ok"]:
            _logger.error("Unable to send text to slack channel.")
