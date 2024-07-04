# Standard
from typing import List
# External
import boto3

class EC2():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client('ec2')

    def stop(self, instance_ids: List[str], hibernate: bool = False, force: bool = False):
        kwargs = {
            'InstanceIds': instance_ids,
            'Hibernate': hibernate,
            'Force': force,
        }
        return self.client.stop_instances(**kwargs)
