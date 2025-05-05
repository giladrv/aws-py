# Standard
import logging
from logging import Handler
from queue import Empty, Queue
from threading import Lock, Thread
import time
# External
import boto3
from botocore.exceptions import ClientError

CLIENT_NAME = 'logs'

MAX_BATCH_SIZE = 1048576
MAX_MESSAGES = 10000
EXTRA_BYTES_PER_MESSAGE = 26
MAX_MESSAGE_SIZE = 262144 - EXTRA_BYTES_PER_MESSAGE # MAX_BATCH_SIZE - EXTRA_BYTES_PER_MESSAGE

def truncate(message: str, max_bytes: int, encoding: str = 'utf-8', prefix = '') -> str:
    encoded = message.encode(encoding = encoding)
    msg_size = len(encoded)
    if len(encoded) <= max_bytes:
        return message, msg_size
    encoded = prefix.encode(encoding = encoding) + encoded
    truncated = encoded[:max_bytes]
    msg_size = max_bytes
    while True:
        try:
            return truncated.decode('utf-8'), msg_size
        except UnicodeDecodeError:
            truncated = truncated[:-1]
            msg_size -= 1

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
            batch_wait: int = 5,
        ):
        super().__init__(level)
        self.client = client or boto3.client(CLIENT_NAME)
        self.log_group = log_group
        self.log_stream = log_stream
        self.batch = []
        self.batch_lock = Lock()
        self.batch_size = 0
        self.batch_wait = batch_wait
        self.queue = Queue()
        self.ensure_resources()
        self.thread = Thread(
            target = self.monitor_queue,
            daemon = True,
            name = f'cwpub-{name}'
        )
        self.thread.start()

    def emit(self, record):
        msg, sz = truncate(
            self.format(record), 
            max_bytes = MAX_MESSAGE_SIZE,
            prefix = '<TRUNCATED>')
        ts = int(record.created * 1000)
        self.flush_batch(sz + EXTRA_BYTES_PER_MESSAGE)
        with self.batch_lock:
            self.batch.append({
                'message': msg,
                'timestamp': ts,
            })
            self.batch_size += sz

    def ensure_resources(self):
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

    def flush(self):
        self.flush_batch(0)
        while True:
            try:
                events = self.queue.get_nowait()
            except Empty as e:
                break
            self.put_batch(events)

    def flush_batch(self, sz: int):
        with self.batch_lock:
            if self.batch and (sz == 0 or len(self.batch) >= MAX_MESSAGES or self.batch_size + sz >= MAX_BATCH_SIZE):
                self.queue.put(self.batch)
                self.batch = []
                self.batch_size = 0

    def monitor_queue(self):
        while True:
            time.sleep(self.batch_wait)
            self.flush_batch(0)
            try:
                events = self.queue.get_nowait()
            except Empty as e:
                continue
            self.put_batch(events)

    def put_batch(self, events):
        kwargs = {
            'logGroupName': self.log_group,
            'logStreamName': self.log_stream,
            'logEvents': events,
        }
        try:
            res = self.client.put_log_events(**kwargs)
        except Exception as e:
            logging.error(f'failed to publish logs: {e}')
