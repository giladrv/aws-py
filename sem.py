# Standard
import json
# External
import boto3

CLIENT_NAME = 'secretsmanager'

class SEM():
    
    def __init__(self,
            client = None,
            profile: str = None):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)
    
    def get_secret_string(self, secret_id: str):
        return self.client.get_secret_value(SecretId = secret_id)['SecretString']

    def get_secret_json(self, secret_id: str, key: str = None):
        secret = json.loads(self.get_secret_string(secret_id))
        if key is None:
            return secret
        else:
            return secret[key]
