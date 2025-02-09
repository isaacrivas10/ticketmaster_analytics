class Authenticator:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_params(self) -> dict:
        return {"apikey": self.api_key}
