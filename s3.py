# Standard
from collections import deque
from enum import Enum
import os
from queue import Queue, Empty as QEmpty
from threading import Thread
from typing import Any, Callable, Dict, Iterable, List
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

def exhaust(generator):
    deque(generator, maxlen = 0)

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

    def copy(self, key: str, new_key: str,
            dst_bucket: str = None,
            src_bucket: str = None):
        kwargs = {
            'Bucket': self.get_request_bucket(dst_bucket),
            'CopySource': {
                'Bucket': self.get_request_bucket(src_bucket),
                'Key': key,
            },
            'Key': new_key,
        }
        return self.client.copy_object(**kwargs)
    
    def count_objects(self, prefix: str,
            bucket: str = None,
            requester: bool = None,
            extra_kwargs: dict = None) -> list:
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

    def delete_keys(self, keys: Iterable[str], bucket: str = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Delete': {},
        }
        deleted = []
        failed = []
        for i in range(0, len(keys), 1000):
            kwargs['Delete']['Objects'] = [ { 'Key': key } for key in keys[i:i+1000] ]
            res = self.client.delete_objects(**kwargs)
            deleted.extend(res.get('Deleted', []))
            failed.extend(res.get('Errors', []))
        return deleted, failed

    def delete_prefix(self, prefix: str):
        deleted = []
        failed = []
        def action(objs: Iterable[dict]):
            b_del, b_fail = self.delete_keys([ obj['Key'] for obj in objs ])
            deleted.extend(b_del)
            failed.extend(b_fail)
        exhaust(self.iterate_objects(prefix = prefix, batch_action = action))
        return deleted, failed

    def download(self, key: str,
            bucket: str = None,
            filepath: str = None,
            verbosity: int = 0,
        ):
        if filepath is None:
            filepath = key.split('/')[-1]
        else:
            if filepath.endswith('/'):
                filepath = filepath + key.split('/')[-1]
            dirname = os.path.dirname(filepath)
            if dirname != '':
                os.makedirs(dirname, exist_ok = True)
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'Filename': filepath,
        }
        if verbosity == 1:
            print(f'Downloading\t{key}')
        elif verbosity == 2:
            print(f'Downloading\ts3://{kwargs["Bucket"]}/{key}\n\t=>\t{filepath}')
        self.client.download_file(**kwargs)

    def download_many(self, params_list: Iterable[str | dict],
            thread_count: int = 4,
            verbosity: int = 0):
        # Validate thread count
        assert thread_count >= 1, 'Thread count must be greater than or equal to 1'
        assert thread_count <= 8, 'Thread count must be less than or equal to 8'
        # Put params_list into a queue
        params_queue = Queue()
        for params in params_list:
            params_queue.put(params)
        # Define the worker function for downloading
        def download_worker():
            while True:
                try:
                    kwargs = params_queue.get_nowait()
                    if isinstance(kwargs, str):
                        kwargs = { 'key': kwargs }
                    elif not isinstance(kwargs, dict):
                        raise ValueError('Params must be a string or a dict with "key" and optional "bucket" and "filepath"')
                    kwargs['verbosity'] = verbosity
                    self.download(**kwargs)
                except QEmpty:
                    break
        # Create and start threads
        threads: List[Thread] = []
        for _ in range(thread_count):
            thread = Thread(target = download_worker)
            thread.start()
            threads.append(thread)
        # Wait for all threads to finish
        for thread in threads:
            thread.join()

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

    def iterate_objects(self,
            prefix: str = None,
            bucket: str = None,
            requester: bool = None,
            extra_kwargs: dict = None,
            object_map: Callable = None,
            batch_action: Callable = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
        }
        if prefix is not None:
            kwargs['Prefix'] = prefix
        if extra_kwargs is not None:
            kwargs.update(extra_kwargs)
        self.add_request_payer(kwargs, requester)
        while True:
            res = self.client.list_objects_v2(**kwargs)
            if res['KeyCount'] > 0:
                if batch_action is not None:
                    batch_action(res['Contents'])
                if object_map is None:
                    for obj in res['Contents']:
                        yield obj
                else:
                    for obj in res['Contents']:
                        obj_res = object_map(obj)
                        if obj_res is not None:
                            yield obj
            if res['IsTruncated']:
                kwargs['ContinuationToken'] = res['NextContinuationToken']
            else:
                break

    def list_keys(self, prefix: str,
            bucket: str = None,
            requester: bool = None) -> List[str]:
        contents = self.list_objects(prefix, bucket, requester)
        return [ obj['Key'] for obj in contents ]

    def list_objects(self, prefix: str,
            bucket: str = None,
            requester: bool = None,
            extra_kwargs: dict = None) -> list:
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
            bucket: str = None,
            content_type: str = None):
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'Body': body,
        }
        if content_type is not None:
            kwargs['ContentType'] = content_type
        self.client.put_object(**kwargs)

    def rename(self, key: str, new_key: str,
            dst_bucket: str = None,
            src_bucket: str = None):
        self.copy(key, new_key, dst_bucket = dst_bucket, src_bucket = src_bucket)
        self.delete_keys([ key ], bucket = src_bucket)

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
            requester: bool = None,
            meta: Dict[str, str] = None,
            content_type: str = None):
        extra_args = {}
        if meta is not None:
            extra_args['Metadata'] = meta
        if content_type is not None:
            extra_args['ContentType'] = content_type
        kwargs = {
            'Bucket': self.get_request_bucket(bucket),
            'Key': key,
            'Filename': filename,
            'ExtraArgs': extra_args
        }
        self.add_request_payer(extra_args, requester)
        return self.client.upload_file(**kwargs)
