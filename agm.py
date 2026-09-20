# Standard
import json
# External
import boto3
from botocore.exceptions import ClientError

CLIENT_NAME = 'apigatewaymanagementapi'

class AGM():

    def __init__(self, client = None, **kwargs):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME, **kwargs)

    def post(self, conn_id: str, payload):
        if isinstance(payload, str):
            data = payload
        else:
            data = json.dumps(payload, default = str)
        try:
            self.client.post_to_connection(
                ConnectionId = conn_id,
                Data = data,
            )
            return True
        except ClientError as e:
            print('ERR', json.dumps(e.response))
            if e.response['Error']['Code'] == 'GoneException':
                return False
            else:
                raise
