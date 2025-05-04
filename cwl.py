# Standard
import logging
from logging import Handler
from queue import Queue
from threading import Thread
import time
# External
import boto3
from botocore.exceptions import ClientError

CLIENT_NAME = 'logs'

class CWL():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)

class CloudWatchHandler(Handler):

    def __init__(self,
            log_group: str,
            log_stream: str,
            client = None,
            level: int = logging.INFO,
            name = 'CloudWatchHandler',
            batch_interval: int = 5,
            max_batch_size: int = 10,
        ):
        super().__init__(level)
        self.client = client or boto3.client(CLIENT_NAME)
        self.log_group = log_group
        self.log_stream = log_stream
        self.batch_interval = batch_interval
        self.max_batch_size = max_batch_size
        self.queue = Queue()
        self._sequence_token = None
        self._ensure_resources()
        thread = Thread(
            target = self._publish,
            daemon = True,
            name = f'cwpub-{name}'
        )
        thread.start()

    def emit(self, record):
        self.queue.put({
            'message': self.format(record),
            'timestamp': int(record.created * 1000),
        })

    def _ensure_resources(self):
        try:
            self.client.create_log_group(logGroupName = self.log_group)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise
        try:
            self.client.create_log_stream(
                logGroupName = self.log_group,
                logStreamName = self.log_stream,
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise

    def _publish(self):
        while True:
            time.sleep(self.batch_interval)
            events = []
            while not self.queue.empty() and len(events) < self.max_batch_size:
                events.append(self.queue.get())
            if len(events) == 0:
                continue
            kwargs = {
                'logGroupName': self.log_group,
                'logStreamName': self.log_stream,
                'logEvents': events,
            }
            if self._sequence_token:
                kwargs['sequenceToken'] = self._sequence_token
            res = self.client.put_log_events(**kwargs)
            self._sequence_token = res['nextSequenceToken']
