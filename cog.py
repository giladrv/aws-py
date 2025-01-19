# Standard
import base64
import binascii
from datetime import UTC, datetime
import hashlib
import hmac
import os
import re
from typing import Any, Dict
# External
import boto3

COG_ACTIONS = [
    'confirm_user'
    'signin',
    'signup',
]

CLIENT_NAME = 'cognito-idp'

N_HEX = 'FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1' \
      + '29024E088A67CC74020BBEA63B139B22514A08798E3404DD' \
      + 'EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245' \
      + 'E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED' \
      + 'EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D' \
      + 'C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F' \
      + '83655D23DCA3AD961C62F356208552BB9ED529077096966D' \
      + '670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B' \
      + 'E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9' \
      + 'DE2BCBF6955817183995497CEA956AE515D2261898FA0510' \
      + '15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64' \
      + 'ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7' \
      + 'ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B' \
      + 'F12FFA06D98A0864D87602733EC86A64521F2B18177B200C' \
      + 'BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31' \
      + '43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF' # https://github.com/aws/amazon-cognito-identity-js/blob/master/src/AuthenticationHelper.js#L22

G_HEX = '2' # https://github.com/aws/amazon-cognito-identity-js/blob/master/src/AuthenticationHelper.js#L49

INFO_BITS = bytearray('Caldera Derived Key', 'utf-8')

def calculate_large_a(g, small_a, big_n):
    big_a = pow(g, small_a, big_n)
    if (big_a % big_n) == 0:
        raise ValueError('Safety check for A failed')
    return big_a

def calculate_u(big_a, big_b):
    u_hex_hash = hex_hash(pad_hex(big_a) + pad_hex(big_b))
    return hex_to_long(u_hex_hash)

def compute_hkdf(ikm, salt):
    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
    info_bits_update = INFO_BITS + bytearray(chr(1), 'utf-8')
    hmac_hash = hmac.new(prk, info_bits_update, hashlib.sha256).digest()
    return hmac_hash[:16]

def custom_read(a: Dict[str, str]):
    return { k: v[7:] for k, v in a.items() if v.startswith('custom:') }

def custom_write(d: Dict[str, str]):
    return [ { 'Name': f'custom:{k}', 'Value': str(v) } for k, v in d.items() ]

def generate_small_a(big_n):
    random_long_int = get_random(128)
    return random_long_int % big_n

def get_random(nbytes):
    random_hex = binascii.hexlify(os.urandom(nbytes))
    return hex_to_long(random_hex)

def hash_sha256(buf):
    a = hashlib.sha256(buf).hexdigest()
    return (64 - len(a)) * '0' + a

def hex_hash(hex_string):
    return hash_sha256(bytearray.fromhex(hex_string))

def hex_to_long(hex_string):
    return int(hex_string, 16)

def lambda_handler(event, context):
    print('EVENT', event)
    cog = COG()
    method = getattr(cog, event['method'])
    res = method(*event.get('args', []), **event.get('kwargs', {}))
    print('RESULT', res)
    return res

def long_to_hex(long_num):
    return '%x' % long_num

def pad_hex(long_int):
    if not isinstance(long_int, str):
        hash_str = long_to_hex(long_int)
    else:
        hash_str = long_int
    if len(hash_str) % 2 == 1:
        hash_str = f'0{hash_str}'
    elif hash_str[0] in '89ABCDEFabcdef':
        hash_str = f'00{hash_str}'
    return hash_str

class SRP():

    def __init__(self, pool_id: str, password: str):
        self.big_n = hex_to_long(N_HEX)
        self.g = hex_to_long(G_HEX)
        self.k = hex_to_long(hex_hash('00' + N_HEX + '0' + G_HEX))
        self.small_a = generate_small_a(self.big_n)
        self.large_a = calculate_large_a(self.g, self.small_a, self.big_n)
        self.pool_name = pool_id.split('_')[1]
        self.password = password
    
    def get_a(self):
        return long_to_hex(self.large_a)

    def get_auth_key(self, username, server_b_value, salt):
        u_value = calculate_u(self.large_a, server_b_value)
        if u_value == 0:
            raise ValueError('U cannot be zero.')
        userpass = f"{self.pool_name}{username}:{self.password}"
        userpass_hash = hash_sha256(userpass.encode('utf-8'))
        x_value = hex_to_long(hex_hash(pad_hex(salt) + userpass_hash))
        g_mod_pow_xn = pow(self.g, x_value, self.big_n)
        int_value2 = server_b_value - self.k * g_mod_pow_xn
        s_value = pow(int_value2, self.small_a + u_value * x_value, self.big_n)
        hkdf = compute_hkdf(bytearray.fromhex(pad_hex(s_value)),
                            bytearray.fromhex(pad_hex(long_to_hex(u_value))))
        return hkdf

    def process_challenge(self, challenge_params: dict):
        user_id_for_srp = challenge_params['USER_ID_FOR_SRP']
        salt_hex = challenge_params['SALT']
        srp_b_hex = challenge_params['SRP_B']
        secret_block_b64 = challenge_params['SECRET_BLOCK']
        timestamp = datetime.now(UTC).strftime("%a %b %d %H:%M:%S UTC %Y")
        timestamp = re.sub(r" 0(\d) ", r" \1 ", timestamp) # strip leading zero from day number (required by AWS Cognito)
        hkdf = self.get_auth_key(user_id_for_srp, hex_to_long(srp_b_hex), salt_hex)
        secret_block_bytes = base64.standard_b64decode(secret_block_b64)
        msg = bytearray(self.pool_name, 'utf-8') \
            + bytearray(user_id_for_srp, 'utf-8') \
            + bytearray(secret_block_bytes) + bytearray(timestamp, 'utf-8')
        hmac_obj = hmac.new(hkdf, msg, digestmod = hashlib.sha256)
        signature_string = base64.standard_b64encode(hmac_obj.digest())
        response = {
            'TIMESTAMP': timestamp,
            'USERNAME': user_id_for_srp,
            'PASSWORD_CLAIM_SECRET_BLOCK': secret_block_b64,
            'PASSWORD_CLAIM_SIGNATURE': signature_string.decode('utf-8') }
        return response

class COG():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)

    def confirm_forgot(self, client_id: str, name: str, code: str, password: str):
        self.client.confirm_forgot_password(
            ClientId = client_id,
            Username = name,
            ConfirmationCode = code,
            Password = password)

    def confirm_user(self, client_id: str, name: str, code: str):
        self.client.confirm_sign_up(
            ClientId = client_id,
            Username = name,
            ConfirmationCode = code)

    def create_user(self, user_pool: str, user_name: str, email: str, custom: Dict[str, Any] = None):
        attributes = [ { 'Name': 'email', 'Value': email } ]
        if custom is not None:
            attributes.extend(custom_write(custom))
        return self.client.admin_create_user(
            UserPoolId = user_pool,
            Username = user_name,
            UserAttributes = attributes)['User']
    
    def delete_user(self, user_pool: str, user_name: str):
        self.client.admin_delete_user(
            UserPoolId = user_pool,
            Username = user_name)

    def forgot_password(self, client_id: str, user_name: str):
        return self.client.forgot_password(
            ClientId = client_id,
            Username = user_name)

    def get_client(self, user_pool_id: str, name: str):
        kwargs = { 'UserPoolId': user_pool_id, 'MaxResults': 60 }
        while True:
            res = self.client.list_user_pool_clients(**kwargs)
            if 'UserPoolClients' not in res:
                return None
            for client in res['UserPoolClients']:
                if client['ClientName'] == name:
                    return client
            kwargs['NextToken'] = res.get('NextToken', None)
            if kwargs['NextToken'] is None:
                return None
    
    def get_user_pool(self, name: str):
        kwargs = { 'MaxResults': 60 }
        while True:
            res = self.client.list_user_pools(**kwargs)
            if 'UserPools' not in res:
                return None
            for user_pool in res['UserPools']:
                if user_pool['Name'] == name:
                    return user_pool
            kwargs['NextToken'] = res.get('NextToken', None)
            if kwargs['NextToken'] is None:
                return None
    
    def resend_confirmation(self, client_id: str, username: str):
        kwargs = {
            'ClientId': client_id,
            'Username': username,
        }
        return self.client.resend_confirmation_code(**kwargs)
    
    def sign_in(self, pool_id: str, client_id: str, username: str, password: str):
        srp = SRP(pool_id, password)
        res = self.client.initiate_auth(
            AuthFlow = 'USER_SRP_AUTH',
            AuthParameters = { 'USERNAME': username, 'SRP_A': long_to_hex(srp.large_a) },
            ClientId = client_id
        )
        challenge = res['ChallengeName']
        if challenge == 'PASSWORD_VERIFIER':
            challenge_res = srp.process_challenge(res['ChallengeParameters'])
            res = self.client.respond_to_auth_challenge(
                ClientId = client_id,
                ChallengeName = challenge,
                ChallengeResponses = challenge_res)
            if res.get('ChallengeName') == 'NEW_PASSWORD_REQUIRED':
                raise PermissionError('Change password before authenticating')
            return res['AuthenticationResult']
        else:
            raise NotImplementedError(f'The challenge "{challenge}" is not supported')

    def sign_up(self, client_id: str, username: str, email: str, password: str):
        kwargs = {
            'ClientId': client_id,
            'Username': username,
            'Password': password,
            'UserAttributes': [ { 'Name': 'email', 'Value': email } ],
        }
        res = self.client.sign_up(**kwargs)
        return res['UserConfirmed']

    def update_custom_attributes(self, pool_id: str, username: str, attributes: Dict[str, str]):
        kwargs = {
            'UserAttributes': custom_write(attributes),
            'Username': username,
            'UserPoolId': pool_id,
        }
        self.client.admin_update_user_attributes(**kwargs)
