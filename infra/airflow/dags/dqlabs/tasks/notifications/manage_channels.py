"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import requests
import json
import os

from dqlabs.utils.notifications import get_teams_config, update_teams_config


def check_team_token(dag_info: dict):
    teams_channel_detail = get_teams_config(dag_info)
    config = teams_channel_detail.get(
        'config', None) if teams_channel_detail else None
    id = teams_channel_detail.get('id', None) if teams_channel_detail else None

    if config and id:
        payload = {
            "client_id": os.environ.get("MS_TEAMS_CLIENT_ID"),
            "refresh_token": config.get("refresh_token"),
            "scope": "User.Read User.ReadBasic.All Group.Read.All ChannelMessage.Send offline_access",
            "grant_type": "refresh_token"
        }

        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Origin": "http://localhost"
        }

        url = f"https://login.microsoftonline.com/common/oauth2/v2.0/token"
        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            auth_tokens = json.loads(response.text)
            if auth_tokens:
                updated_token = auth_tokens.get('access_token', None)
                updated_refresh_token = auth_tokens.get('refresh_token', None)
                if updated_token and updated_refresh_token:
                    config.update({
                        "access_token": updated_token,
                        "refresh_token": updated_refresh_token
                    })
                    update_teams_config(dag_info, id, config)
