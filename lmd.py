# Standard
from enum import Enum
import json
# External
import boto3
from botocore.config import Config
# Internal
from . import enval

DEF_KWARGS = {
    'config': Config(read_timeout = 600, retries = {'max_attempts': 0}, region_name = 'ap-southeast-1'),
}

CLIENT_NAME = 'lambda'

class InvokeType(Enum):
    DRY = 'DryRun'
    EVT = 'Event'
    REQ = 'RequestResponse'

class OutputType(Enum):
    Raw = 0
    Payload = 1
    JsonPayload = 2
    Body = 3
    JsonBody = 4

def out_raw(response):
    return response

def out_body(response):
    return out_json_payload(response)['body']

def out_json_body(response):
    return json.loads(out_body(response))

def out_json_payload(response):
    return json.loads(out_payload(response))

def out_payload(response):
    return response["Payload"].read()

OUT_FUN = {
    OutputType.Raw: out_raw,
    OutputType.Payload: out_payload,
    OutputType.JsonPayload: out_json_payload,
    OutputType.Body: out_body,
    OutputType.JsonBody: out_json_body,
}

class LMD():

    def __init__(self, client = None, profile: str = None, kwargs: dict = None, prefix: str = None):
        if kwargs is None:
            kwargs = DEF_KWARGS
        else:
            kwargs = { **DEF_KWARGS, **kwargs }
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME, **kwargs)
        else:
            self.client = boto3.client(CLIENT_NAME, **kwargs)
        self.prefix = prefix

    def invoke(self, name: str, payload: dict, invoke_type: InvokeType):
        if self.prefix is not None:
            name = self.prefix + name
        kwargs = {
            'FunctionName': name,
            'Payload': json.dumps(payload, default = str),
            'InvocationType': enval(invoke_type),
        }
        return self.client.invoke(**kwargs)
    
    def event(self, name: str, payload: dict):
        return self.invoke(name, payload, InvokeType.EVT)

    def request(self, name: str, payload: dict, output_type: OutputType = OutputType.JsonBody):
        response = self.invoke(name, payload, InvokeType.REQ)
        return OUT_FUN[output_type](response)
