# Standard
import requests
# Internal
from cog import SRP

class IDP():

    def __init__(self, region: str,
            pool_id: str | None = None,
            client_id: str | None = None):
        self.region = region
        self.url = f'https://cognito-idp.{region}.amazonaws.com'
        self.pool_id = pool_id
        self.client_id = client_id

    def sign_in(self, username: str, password: str,
            pool_id: str | None = None,
            client_id: str | None = None):
        if pool_id is None:
            pool_id = self.pool_id
        if client_id is None:
            client_id = self.client_id
        srp = SRP(pool_id, password)
        headers = {
            'X-Amz-Target': 'AWSCognitoIdentityProviderService.InitiateAuth',
            'Content-Type': 'application/x-amz-json-1.1',
        }
        body = {
            "AuthFlow" : "USER_SRP_AUTH",
            "AuthParameters" : {
                "USERNAME" : username,
                "SRP_A" : srp.get_a(),
            },
            "ClientId" : client_id,
        }
        res: dict = requests.post(self.url, json = body, headers = headers).json()
        challenge_name = res.get('ChallengeName')
        if challenge_name == 'PASSWORD_VERIFIER':
            challenge_res = srp.process_challenge(res['ChallengeParameters'])
            headers = {
                'X-Amz-Target': 'AWSCognitoIdentityProviderService.RespondToAuthChallenge',
                'Content-Type': 'application/x-amz-json-1.1',
            }
            body = {
                "ChallengeName" : challenge_name,
                "ChallengeResponses" : challenge_res,
                "ClientId" : client_id,
            }
            res: dict = requests.post(self.url, json = body, headers = headers).json()
            if res.get('ChallengeName') == 'NEW_PASSWORD_REQUIRED':
                raise PermissionError('Change password before authenticating')
            return res['AuthenticationResult']
        else:
            raise NotImplementedError(f'The challenge "{challenge_name}" is not supported')

    def sign_up(self, username: str, email: str, password: str,
            client_id: str | None = None) -> dict:
        if client_id is None:
            client_id = self.client_id
        headers = {
            'X-Amz-Target': 'AWSCognitoIdentityProviderService.SignUp',
            'Content-Type': 'application/x-amz-json-1.1',
        }
        body = {
            'ClientId': client_id,
            'Username': username,
            'Password': password,
            'UserAttributes': [{
                'Name': 'email',
                'Value': email,
            }],
        }
        return requests.post(self.url, json = body, headers = headers).json()
