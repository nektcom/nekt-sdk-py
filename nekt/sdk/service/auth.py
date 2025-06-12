import requests


def get_cloud_credentials(data_access_token: str) -> dict:
    """
    Get the cloud credentials from the Nekt API.
    """
    resp = requests.get(
        "https://api.nekt.ai/api/v1/jupyter-credentials/",
        headers={"X-Jupyter-Token": data_access_token},
    )
    return resp.json()
