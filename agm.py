# External
import boto3

CLIENT_NAME = 'apigatewaymanagementapi'

class AGM():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)
