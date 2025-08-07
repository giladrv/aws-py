# Standard
from enum import Enum
import os
import time
from typing import Any, Callable, Dict, List
from urllib.parse import quote as urlquote
# External
import boto3
from botocore.exceptions import ClientError

EVENTS_URL = 'https://{region}.console.aws.amazon.com/cloudformation/home?region={region}#/stacks/events?stackId={stack}'

INVOKE_DEST = "{ Destination: !Ref {resource} }"
LAYER_PREFIX = "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:layer"
NO_VALUE = "!Ref AWS::NoValue"

CLIENT_NAME = 'cloudformation'

class Capability(Enum):
    AutoExpand = 'CAPABILITY_AUTO_EXPAND'
    IAM = 'CAPABILITY_IAM'
    NamedIAM = 'CAPABILITY_NAMED_IAM'

def extract_outputs(stack_details: dict):
    return { o['OutputKey']: o['OutputValue'] for o in stack_details['Outputs'] }

def extract_parameters(stack_details: dict):
    return { p['ParameterKey']: p['ParameterValue'] for p in stack_details['Parameters'] }

def get_stack_events_url(region: str, stack_id: str):
    return EVENTS_URL.format(
        region = region,
        stack = urlquote(stack_id, safe = ''))

class CFN():

    def __init__(self, client = None,
            capability_named_iam: bool = None,
            capability_auto_expand: bool = False,
            wait_delay: float = 10):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)
        self.capability_named_iam = capability_named_iam
        self.capability_auto_expand = capability_auto_expand
        self.wait_delay = wait_delay

    def describe_events(self, stack: str):
        kwargs = { 'StackName': stack }
        events = []
        cont = True
        while cont:
            res = self.client.describe_stack_events(**kwargs)
            for event in res['StackEvents']:
                events.append(event)
                if event.get('ResourceStatusReason', None) == 'User Initiated':
                    cont = False
                    break
            kwargs['NextToken'] = res.get('NextToken', None)
            if kwargs['NextToken'] is None:
                break
        return list(reversed(events))

    def describe_resource(self, stack: str, resource: str):
        kwargs = {
            'StackName': stack,
            'LogicalResourceId': resource,
        }
        return self.client.describe_stack_resource(**kwargs)['StackResourceDetail']

    def describe_stack(self, name: str):
        return self.client.describe_stacks(StackName = name)['Stacks'][0]

    def update_stack(self, name: str, bucket: str, key: str,
            params: Dict[str, Any] = None,
            ignore_nochange: bool = True):
        kwargs = {
            "StackName": name,
            "TemplateURL": f"https://s3.amazonaws.com/{bucket}/{key}",
            "Capabilities": []
        }
        if params is not None:
            kwargs["Parameters"] = []
            for pkey, pval in params.items():
                p = { "ParameterKey": pkey }
                if pval is None:
                    p["UsePreviousValue"] = True
                else:
                    p["ParameterValue"] = str(pval)
                kwargs["Parameters"].append(p)
        if self.capability_named_iam == True:
            kwargs["Capabilities"].append(Capability.NamedIAM.value)
        elif self.capability_named_iam == False:
            kwargs["Capabilities"].append(Capability.IAM.value)
        if self.capability_auto_expand == True:
            kwargs["Capabilities"].append(Capability.AutoExpand.value)
        try:
            res = self.client.update_stack(**kwargs)
            return res["StackId"]
        except ClientError as e:
            if ignore_nochange and e.response['Error']['Message'] == 'No updates are to be performed.':
                return name
            raise

    def wait(self, name: str, done_status: List[str], loop_status: List[str],
            callback: Callable[[Dict[str, Any]], None] = None):
        while True:
            time.sleep(self.wait_delay)
            stack = self.describe_stack(name)
            status = stack['StackStatus']
            if status in done_status:
                return stack
            if status not in loop_status:
                raise Exception(status)
            if callback is not None:
                callback(stack)

    def wait_create(self, name: str, callback: Callable[[Dict[str, Any]], None] = None):
        return self.wait(name, ['CREATE_COMPLETE'], ['CREATE_IN_PROGRESS'], callback = callback)

    def wait_update(self, name: str, callback: Callable[[Dict[str, Any]], None] = None):
        return self.wait(name, ['UPDATE_COMPLETE'], ['UPDATE_IN_PROGRESS','UPDATE_COMPLETE_CLEANUP_IN_PROGRESS'], callback = callback)
