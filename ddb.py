# Standard
from typing import List
# External
import boto3

CLIENT_NAME = 'dynamodb'

class DDB():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)
