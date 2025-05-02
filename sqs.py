# Standard
import json
# External
import boto3

CLIENT_NAME = 'sqs'

class SQS():
    
    def __init__(self, url: str,
            client = None,
            profile: str = None
        ):
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)
        self.url = url

    def delete(self, receipt_handle: str):
        kwargs = {
            'QueueUrl': self.url,
            'ReceiptHandle': receipt_handle,
        }
        self.client.delete_message(**kwargs)

    def receive(self, max_num: int = 1, wait_time: int = 0):
        kwargs = {
            'QueueUrl': self.url,
            'MaxNumberOfMessages': max_num,
            'WaitTimeSeconds': wait_time,
        }
        response: dict = self.client.receive_message(**kwargs)
        return response.get('Messages', [])

    def send(self, message: str, deduplication: str, group: str):
        kwargs = {
            'QueueUrl': self.url,
            'MessageBody': message,
            'MessageDeduplicationId': deduplication,
            'MessageGroupId': group,
        }
        response = self.client.send_message(**kwargs)
        return response['MessageId']
