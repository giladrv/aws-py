# Standard
from enum import Enum
import os
from typing import Any, Callable, Dict, List
# External
import boto3
from botocore.client import Config
# Internal
from . import enval

CLIENT_NAME = 's3'
CLIENT_CONFIG = Config(s3 = { 'addressing_style': 'path' })

class RestoreTier(Enum):
    BLK = 'Bulk'
    STD = 'Standard'
    XPD = 'Expedited'

class StorageClass(Enum):
    STD = 'Standard'
    GLR = 'Glacier'

SCALE_PREFIX = [ '', 'K', 'M', 'G', 'T', 'P', 'E', 'Z' ]

def hsize(size, decimals: int = 1, suffix: str = 'B'):
    scale = 0
    while size > 1024:
        size = size / 1024
        scale += 1
    return f'{round(size, decimals)}{SCALE_PREFIX[scale]}{suffix}'

def stem(path: str):
    return os.path.splitext(os.path.basename(path))[0]

class S3():
    
    def __init__(self,
            client = None,
            profile: str = None,
            bucket: str = None,
            requester: bool = None):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME, config = CLIENT_CONFIG)
        else:
            self.client = boto3.client(CLIENT_NAME, config = CLIENT_CONFIG)
        self.bucket = enval(bucket)
        self.requester = requester
    
    def add_request_payer(self,
            kwargs: Dict[str, Any],
            requester: bool = None):
        if requester is not None:
            if requester == True:
                kwargs['RequestPayer'] = 'requester'
        elif self.requester is not None:
            if self.requester == True:
                kwargs['RequestPayer'] = 'requester'

    def count_objects(self, prefix: str, bucket: str = None, requester: bool = None, extra_kwargs: dict = None) -> list:
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Prefix': prefix,
        }
        if extra_kwargs is not None:
            kwargs.update(extra_kwargs)
        self.add_request_payer(kwargs, requester)
        count = 0
        size = 0
        while True:
            res = self.client.list_objects_v2(**kwargs)
            count += res['KeyCount']
            if res['KeyCount'] > 0:
                size += sum(obj['Size'] for obj in res['Contents'])
            if res['IsTruncated']:
                kwargs['ContinuationToken'] = res['NextContinuationToken']
            else:
                break
        return count, size
    
    def download(self, key: str,
            bucket: str = None,
            filepath: str = None):
        if filepath is None:
            filepath = key.split('/')[-1]
        else:
            dirname = os.path.dirname(filepath)
            if dirname != '':
                os.makedirs(dirname, exist_ok = True)
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'Filename': filepath,
        }
        self.client.download_file(**kwargs)
    
    def get_object(self, key: str,
            bucket: str = None,
            requester: bool = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
        }
        self.add_request_payer(kwargs, requester)
        return self.client.get_object(**kwargs)

    def get_object_as_str(self, key: str,
            bucket: str = None,
            requester: bool = None):
        res = self.get_object(key, bucket = bucket, requester = requester)
        return res['Body'].read().decode()

    def get_object_to_file(self, key: str,
            filepath: str = None,
            bucket: str = None,
            requester: bool = None):
        if filepath is None:
            filepath = key.split('/')[-1]
        res = self.get_object(key, bucket = bucket, requester = requester)
        with open(filepath, 'wb') as fwb:
            for chunk in res['Body'].iter_chunks():
                fwb.write(chunk)

    def get_request_bucket(self, bucket: str = None):
        return bucket if bucket is not None else self.bucket

    def head_object(self, key: str,
            bucket: str = None,
            requester: bool = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
        }
        self.add_request_payer(kwargs, requester)
        return self.client.head_object(**kwargs)

    def list_objects(self, prefix: str, bucket: str = None, requester: bool = None, extra_kwargs: dict = None) -> list:
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Prefix': prefix,
        }
        if extra_kwargs is not None:
            kwargs.update(extra_kwargs)
        self.add_request_payer(kwargs, requester)
        contents = []
        while True:
            res = self.client.list_objects_v2(**kwargs)
            if res['KeyCount'] > 0:
                contents.extend(res['Contents'])
            if res['IsTruncated']:
                kwargs['ContinuationToken'] = res['NextContinuationToken']
            else:
                break
        return contents
    
    def list_keys(self, prefix: str, bucket: str = None, requester: bool = None) -> List[str]:
        contents = self.list_objects(prefix, bucket, requester)
        return [ obj['Key'] for obj in contents ]
    
    def mock_restore(self, key: str, bucket: str = None):
        return {
            'Records': [
                {
                    's3': {
                        'bucket': {
                            'name': self.get_request_bucket(bucket),
                        },
                        'object': {
                            'key': key,
                        }
                    }
                }
            ]
        }

    def put(self, key: str, body: str,
            bucket: str = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'Body': body,
        }
        self.client.put_object(**kwargs)

    def restore_object(self, key: str,
            bucket: str = None,
            days: int = 1,
            tier: RestoreTier = RestoreTier.BLK,
            requester: bool = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'RestoreRequest': {
                'Days': days,
                'GlacierJobParameters': {
                    'Tier': tier.value,
                }
            },
        }
        self.add_request_payer(kwargs, requester)
        return self.client.restore_object(**kwargs)

    def upload(self, filename: str, key: str,
            bucket: str = None,
            requester: bool = None):
        extra_args = {}
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'Filename': filename,
            'ExtraArgs': extra_args
        }
        self.add_request_payer(extra_args, requester)
        return self.client.upload_file(**kwargs)
