from __future__ import annotations

from http import HTTPStatus

import requests
from requests.exceptions import HTTPError


def load_secret(data_access_token: str, key: str, api_url: str = "https://api.nekt.ai") -> str:
    """
    Get a secret value by key from the Nekt API.
    """
    url = f"{api_url}/api/v1/organization/secrets/{key}/"
    headers = {"X-Jupyter-Token": data_access_token}
    resp = requests.get(url, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        return data.get("value")
    elif resp.status_code == HTTPStatus.NOT_FOUND:
        raise HTTPError(f"Secret not found: {key}")
    elif resp.status_code == HTTPStatus.FORBIDDEN:
        raise HTTPError(f"You not have access to this secret: {key}")
    else:
        raise HTTPError(f"Failed to get secret: {resp.text}")
