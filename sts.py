# External
import boto3

CLIENT_NAME = 'sts'

class STS():
    
    def __init__(self,
            client = None,
            profile: str = None):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)
    
    def assume_role(self,
            account_id,
            role_name: str,
            session: str = None,
            duration: int = 3600,
        ):
        if session is None:
            session = f'{account_id}-{role_name}'
        kwargs = {
            'RoleArn': f'arn:aws:iam::{account_id}:role/{role_name}',
            'RoleSessionName': session,
            'DurationSeconds': duration,
        }
        return self.client.assume_role(**kwargs)

    def get_role_creds(self,
            account_id,
            role_name: str,
            session: str = None,
            duration: int = 3600,
        ):
        res = self.assume_role(account_id, role_name, session = session, duration = duration)
        credentials = res['Credentials']
        kwargs = {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken'],
        }
        return kwargs

    def get_client_with_role(self,
            client_name: str,
            account_id,
            role_name: str,
            session: str = None,
            duration: int = 3600,
            client_kwargs: dict = {}):
        creds = self.get_role_creds(account_id, role_name, session = session, duration = duration)
        return boto3.client(client_name, **creds, **client_kwargs)
