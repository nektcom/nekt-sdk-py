import requests


def get_cloud_credentials(data_access_token: str, api_url: str = "https://api.nekt.ai") -> dict:
    """
    Get the cloud credentials from the Nekt API.
    """
    resp = requests.get(
        f"{api_url}/api/v1/jupyter-credentials/",
        headers={"X-Jupyter-Token": data_access_token},
    )
    return resp.json()
