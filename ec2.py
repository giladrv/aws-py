# Standard
from typing import List
# External
import boto3

CLIENT_NAME = 'ec2'

class EC2():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)
        self.default_security_group: dict = None
        self.default_subnets: dict = None
        self.default_vpc: dict = None

    def describe_instance(self, instance_id: str) -> dict:
        res = self.describe_instances([ instance_id ])
        if instance_id not in res:
            raise Exception(f'Instance {instance_id} not found')
        return res[instance_id]

    def describe_instances(self, ids: List[str]) -> dict:
        res = self.client.describe_instances(InstanceIds = ids)
        return {
            instance['InstanceId']: instance
            for reservation in res.get('Reservations', [])
            for instance in reservation.get('Instances', [])
        }

    def get_default_vpc_id(self) -> str:
        if self.default_vpc is None:
            filters = [{ 'Name': 'is-default', 'Values': [ 'true' ] }]
            self.default_vpc = self.client.describe_vpcs(Filters = filters)['Vpcs'][0]
        return self.default_vpc['VpcId']

    def get_default_security_group_id(self) -> str:
        if self.default_security_group is None:
            filters = [
                { 'Name': 'vpc-id', 'Values': [ self.get_default_vpc_id() ] },
                { 'Name': 'group-name', 'Values': [ 'default' ] },
            ]
            res = self.client.describe_security_groups(Filters = filters)
            self.default_security_group = res['SecurityGroups'][0]
        return self.default_security_group['GroupId']

    def get_default_subnets_ids(self) -> List[str]:
        if self.default_subnets is None:
            filters = [{ 'Name': 'vpc-id', 'Values': [ self.get_default_vpc_id() ] }]
            self.default_subnets = self.client.describe_subnets(Filters = filters)['Subnets']
        return [ subnet['SubnetId'] for subnet in self.default_subnets ]

    def start_instances(self, ids: str | List[str]):
        if isinstance(ids, str):
            ids = [ ids ]
        res = self.client.start_instances(InstanceIds = ids)
        return { instance['InstanceId']: {
            'current': instance['CurrentState']['Name'],
            'previous': instance['PreviousState']['Name'],
        } for instance in res.get('StartingInstances', []) }

    def stop_instances(self, ids: str | List[str], hibernate: bool = False):
        if isinstance(ids, str):
            ids = [ ids ]
        kwargs = {
            'InstanceIds': ids,
            'Hibernate': hibernate,
        }
        self.client.stop_instances(**kwargs)
