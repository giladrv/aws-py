# Standard
from datetime import datetime, timezone
from typing import Any, Dict, Iterable
# External
import boto3
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

CLIENT_NAME = 'dynamodb'

class DDB():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(CLIENT_NAME)

ser = TypeSerializer()
des = TypeDeserializer()

def _now():
    return round(datetime.now(timezone.utc).timestamp())

def _serialize(v):
    return ser.serialize(v)

def _unmarshal(item):
    return { k: des.deserialize(v) for k, v in item.items() } if item else None

PK = 'PK'
SK = 'SK'

class TableWithUniques:

    def __init__(self, name: str, id_key: str,
            uq_keys: Iterable[str] = None,
        ):
        self.ddb = boto3.client(CLIENT_NAME)
        self.name = name
        self.id_key = id_key
        self.uq_keys = uq_keys or []

    def _id_key(self, id_val: str):
        return {
            PK: _serialize(self._id_pk(id_val)),
            SK: _serialize(self.id_key.upper())
        }

    def _id_pk(self, id_val: str):
        return f'{self.id_key.upper()}#{id_val}'

    def _item(self, attrs: Dict[str, Any]):
        id_val = attrs[self.id_key]
        now = _now()
        body = {
            PK: self._id_pk(id_val),
            SK: self.id_key.upper(),
            **{ k: v for k, v in attrs.items() if k != self.id_key },
            'created': now,
            'updated': now,
        }
        return _serialize(body)['M']

    def _uq_key(self, uq_key: str, uq_val: str):
        return {
            PK: _serialize(self._uq_pk(uq_key, uq_val)),
            SK: _serialize(SK),
        }

    def _uq_item(self, id_val: str, uq_key: str, uq_val: str):
        return self._uq_key(uq_key, uq_val) | {
            self.id_key: _serialize(id_val),
        }

    def _uq_pk(self, uq_key: str, uq_val: str):
        return f'{uq_key.upper()}#{uq_val}'

    def delete(self, attrs: dict):
        del_args = {
            'TableName': self.name,
            'ConditionExpression': f'attribute_exists({PK})'
        }
        id_val = attrs[self.id_key]
        items = [
            self._id_key(id_val),
            *[ self._uq_key(uq_key, attrs[uq_key]) for uq_key in self.uq_keys ]
        ]
        res = self.ddb.transact_write_items(
            TransactItems = [ { 'Delete': del_args | { 'Key': item } } for item in items ]
        )
        print('res', res)

    def get(self, id_val: str, consistent = True):
        r = self.ddb.get_item(
            TableName = self.name,
            Key = self._id_key(id_val),
            ConsistentRead = consistent
        )
        attrs = _unmarshal(r.get('Item'))
        attrs.pop(PK, None)
        attrs.pop(SK, None)
        attrs[self.id_key] = id_val
        return attrs

    def get_uq(self, uq_key: str, uq_val: str):
        r = self.ddb.query(
            TableName = self.name,
            KeyConditionExpression = f'{PK} = :p',
            ExpressionAttributeValues = {
                ':p': _serialize(self._uq_pk(uq_key, uq_val))
            },
            Limit = 1,
        )
        uq_item = _unmarshal(r.get('Items')[0])
        id_val = uq_item[self.id_key]
        return self.get(id_val)

    def put(self, attrs: Dict[str, Any]):
        put_args = {
            'TableName': self.name,
            'ConditionExpression': f'attribute_not_exists({PK})'
        }
        id_val = attrs[self.id_key]
        items = [
            self._item(attrs),
            *[ self._uq_item(id_val, uq_key, attrs[uq_key]) for uq_key in self.uq_keys ]
        ]
        res = self.ddb.transact_write_items(
            TransactItems = [ { 'Put': put_args | { 'Item': item } } for item in items ]
        )
        print('res', res)

    def put_sk(self, id_val: str, sk: str, attrs: Dict[str, Any]):
        now = _now()
        body = {
            PK: self._id_pk(id_val),
            SK: sk.upper(),
            **attrs,
            'created': now,
            'updated': now
        }
        res = self.ddb.put_item(
            TableName = self.name,
            Item = _serialize(body)['M']
        )
        print('res', res)

    def update(self, id_val: str, attrs: Dict[str, Any]):
        exclude = { PK, SK, self.id_key, 'created' }
        _attrs = { k: v for k, v in attrs.items() if k not in exclude } | { 'updated': _now() }
        sets = [ f'#_{k} = :_{k}' for k in _attrs ]
        names = { f'#_{k}': k for k in _attrs }
        values = { f':_{k}': _serialize(v) for k, v in _attrs.items() }
        res = self.ddb.update_item(
            TableName = self.name,
            Key = self._id_key(id_val),
            UpdateExpression = 'SET ' + ', '.join(sets),
            ExpressionAttributeNames = names,
            ExpressionAttributeValues = values,
            ConditionExpression = f'attribute_exists({PK})',
            ReturnValues = 'UPDATED_OLD' # ALL_NEW, UPDATED_OLD, NONE, ALL_OLD
        )
        print('res', res)
        return _unmarshal(res['Attributes'])

    def update_uq(self, id_val: str, uq_key: str, uq_new: str, uq_old: str):
        tx = [
            {
                'Put': {
                    'TableName': self.name,
                    'Item': self._uq_item(id_val, uq_key, uq_new),
                    'ConditionExpression': f'attribute_not_exists({PK})'
                }
            },
            {
                'Update': {
                    'TableName': self.name,
                    'Key': self._id_key(id_val),
                    'UpdateExpression': f'SET #_{uq_key} = :_{uq_key}, #_updated = :_updated',
                    'ExpressionAttributeNames': {
                        f'#_{uq_key}': uq_key,
                        '#_updated': 'updated'
                    },
                    'ExpressionAttributeValues': {
                        f':_{uq_key}': _serialize(uq_new),
                        ':_updated': _serialize(_now())
                    },
                    'ConditionExpression': f'attribute_exists({PK})'
                }
            },
            {
                'Delete': {
                    'TableName': self.name,
                    'Key': self._uq_item(id_val, uq_key, uq_old),
                }
            },
        ]
        res = self.ddb.transact_write_items(TransactItems = tx)
        print('res', res)
