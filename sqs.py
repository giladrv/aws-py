# Standard
import json
# External
import boto3

CLIENT_NAME = 'sqs'

class SQS():
    
    def __init__(self,
            client = None,
            profile: str = None):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)
