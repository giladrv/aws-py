# External
import boto3

CLIENT_NAME = 'apigateway'

class AGW():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)

    def get_api_key(self, key_id: str) -> str:
        res = self.client.get_api_key(apiKey = key_id, includeValue = True)
        return res['value']
