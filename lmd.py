# Standard
from enum import Enum
import json
import os
import shutil
# External
import boto3
from botocore.config import Config
# Internal
from . import enval

FUNCTION_URL = 'https://{region}.console.aws.amazon.com/lambda/home?region={region}#/functions/{name}'
LOG_GROUP_URL = 'https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group'

DEF_KWARGS = {
    'config': Config(retries = {'total_max_attempts': 1}),
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

def clear_tmp(verbose = False):
    tmp_size = 0
    for filename in os.listdir('/tmp'):
        filepath = os.path.join('/tmp', filename)
        tmp_size += os.path.getsize(filepath)
        try:
            if os.path.isfile(filepath) or os.path.islink(filepath):
                os.unlink(filepath)
            elif os.path.isdir(filepath):
                shutil.rmtree(filepath)
            if verbose:
                print(f'DEL {filename}')
        except Exception as e:
            print(f'ERR {filename} - {e}')
    if verbose:
        print('/tmp', tmp_size)

def encode_fragment(fragment: str):
    from urllib.parse import quote
    return quote(fragment, safe = '').replace('%', '$25')

def get_function_url(name = None, region = None):
    if name is None:
        name = os.environ['AWS_LAMBDA_FUNCTION_NAME']
    if region is None:
        region = os.environ['AWS_REGION']
    return FUNCTION_URL.format(region = region, name = name)

def get_log_stream_url():
    base_url = LOG_GROUP_URL.format(region = os.environ['AWS_REGION'])
    log_group = encode_fragment(os.environ['AWS_LAMBDA_LOG_GROUP_NAME'])
    log_stream = encode_fragment(os.environ['AWS_LAMBDA_LOG_STREAM_NAME'])
    return f'{base_url}/{log_group}/log-events/{log_stream}'

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

    def __init__(self,
            client = None,
            profile: str | None = None,
            region: str | None = None,
            kwargs: dict | None = None,
            prefix: str | None = None):
        if kwargs is None:
            kwargs = DEF_KWARGS
        else:
            kwargs = { **DEF_KWARGS, **kwargs }
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile, region_name = region).client(CLIENT_NAME, **kwargs)
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
