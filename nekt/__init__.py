class TransformationClient:
    def __init__(self, data_access_token: str):
        self.data_access_token = data_access_token

    def transform(self, data: dict) -> dict:
        return data
