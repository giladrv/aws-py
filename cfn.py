# Standard
from enum import Enum
import time
from typing import Any, Callable, Dict, List
# External
import boto3

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

class CFN():

    def __init__(self, client = None,
            capability_named_iam: bool = None,
            wait_delay: float = 10):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)
        self.capability_named_iam = capability_named_iam
        self.wait_delay = wait_delay

    def describe_resource(self, stack: str, resource: str):
        kwargs = {
            'StackName': stack,
            'LogicalResourceId': resource,
        }
        return self.client.describe_stack_resource(**kwargs)['StackResourceDetail']

    def describe_stack(self, name: str):
        return self.client.describe_stacks(StackName = name)['Stacks'][0]

    def update_stack(self, name: str, bucket: str, key: str,
            params: Dict[str, Any] = None):
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
        res = self.client.update_stack(**kwargs)
        return res["StackId"]

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
