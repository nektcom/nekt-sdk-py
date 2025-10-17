from __future__ import annotations

import requests


def get_secret(data_access_token: str, key: str, api_url: str = "https://api.nekt.ai") -> str:
    """
    Get a secret value by key from the Nekt API.
    """
    url = f"{api_url}/api/v1/organization/secrets/{key}/"
    headers = {"X-Jupyter-Token": data_access_token}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value")
