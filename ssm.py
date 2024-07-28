# Standard
import os
# External
import boto3

CLIENT_NAME = 'ssm'

class SSM():
    
    def __init__(self,
            client = None,
            profile: str | None = None):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)

    def get_key_value(self, key_id: str):
        kwargs = {
            'Name': f'/ec2/keypair/{key_id}',
            'WithDecryption': True,
        }
        return self.client.get_parameter(**kwargs)['Parameter']['Value']
    
    def download_key(self, key_id: str, key_file: str):
        with open(key_file, 'w') as f:
            f.write(self.get_key_value(key_id))
        os.chmod(key_file, 0o400)
