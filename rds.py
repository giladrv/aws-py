# External
import boto3

CLIENT_NAME = 'rds'

class RDS():
    
    def __init__(self,
            client = None,
            profile: str = None):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)
    
    def iam_auth(self, db_host: str, db_port: int, iam_user_name: str,
            region: str = None):
        kwargs = {
            'DBHostname': db_host,
            'Port': db_port,
            'DBUsername': iam_user_name
        }
        if region is not None:
            kwargs['Region'] = region
        return self.client.generate_db_auth_token(**kwargs)
