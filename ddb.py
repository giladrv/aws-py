# Standard
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from enum import Enum
from itertools import islice
import json
from typing import Any, Callable, Dict, Iterable, List, NotRequired, TypedDict
# External
import boto3
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

CLIENT_NAME = 'dynamodb'

def _ddb():
    return boto3.client(CLIENT_NAME)

class DDB():

    def __init__(self, client = None):
        if client is not None:
            self.client = client
        else:
            self.client = _ddb()

ser = TypeSerializer()
des = TypeDeserializer()

def _now():
    return round(datetime.now(timezone.utc).timestamp())

def _serialize(v):
    return ser.serialize(v)

def _unmarshal(item):
    return { k: des.deserialize(v) for k, v in item.items() } if item else None

class Encoding(Enum):
    NONE = 0
    DEFAULT = 1

ENCODERS: Dict[Encoding, Callable] = {
    Encoding.NONE: lambda x: x,
    Encoding.DEFAULT: _serialize,
}

class UpdateActionType(Enum):
    ADD = 'ADD'
    DEL = 'DELETE'
    REM = 'REMOVE'
    SET = 'SET'

class UpdateAction(ABC):
    @property
    @abstractmethod
    def clause(self) -> UpdateActionType:
        pass
    @abstractmethod
    def expression(self, key: str):
        pass
    @property
    @abstractmethod
    def value(self):
        pass

class UpdateAdd(UpdateAction):
    def __init__(self, items: set):
        self.items = items
    @property
    def clause(self):
        return UpdateActionType.ADD
    def expression(self, key: str):
        return f'#_{key} :_{key}'
    @property
    def value(self):
        return self.items

class UpdateListAppend(UpdateAction):
    def __init__(self, items: Iterable, beginning: bool = False):
        self.items = items
        self.beginning = beginning
    @property
    def clause(self):
        return UpdateActionType.SET
    def expression(self, key: str):
        a = [ f'#_{key}', f':_{key}' ]
        if self.beginning:
            a.reverse()
        return f'#_{key} = list_append({", ".join(a)})'
    @property
    def value(self):
        return self.items

class RecordTimestamps(TypedDict):
    created: int
    updated: int
    ttl: NotRequired[int]

def new_record(ttl: timedelta = None):
    now = _now()
    rec = RecordTimestamps(created = now, updated = now)
    if ttl is not None:
        rec['ttl'] = int(now + ttl.total_seconds())
    return rec

PK = 'PK'
SK = 'SK'

type Item = Dict[str, Any]

class TableWithUniques:

    def __init__(self, name: str, id_key: str,
            uq_keys: Iterable[str] = None,
        ):
        self.ddb = boto3.client(CLIENT_NAME)
        self.name = name
        self.id_key = id_key
        self.uq_keys = uq_keys or []

    def _id_key(self, id_val: str,
            encoding: Encoding = Encoding.DEFAULT,
        ):
        encoder = ENCODERS[encoding]
        return {
            PK: encoder(self._id_pk(id_val)),
            SK: encoder(self.id_key.upper())
        }

    def _id_pk(self, id_val: str):
        return f'{self.id_key.upper()}#{id_val}'

    def _item(self, attrs: Item):
        id_val = attrs[self.id_key]
        body = self._id_key(id_val, encoding = Encoding.NONE) \
                | { k: v for k, v in attrs.items() if k != self.id_key } \
                | new_record()
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
            TransactItems = [ { 'Delete': del_args | { 'Key': item } } for item in items ],
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', res['ConsumedCapacity'])

    def delete_sk(self, id_val: str, sk: str):
        res = self.ddb.delete_item(
            TableName = self.name,
            Key = {
                PK: _serialize(self._id_pk(id_val)),
                SK: _serialize(sk.upper())
            },
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', res['ConsumedCapacity'])

    def get(self, id_val: str, consistent = True):
        r = self.ddb.get_item(
            TableName = self.name,
            Key = self._id_key(id_val),
            ConsistentRead = consistent,
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', r['ConsumedCapacity'])
        attrs = _unmarshal(r.get('Item'))
        attrs.pop(PK, None)
        attrs.pop(SK, None)
        attrs[self.id_key] = id_val
        return attrs

    def get_many(self, id_vals: Iterable[str], consistent = True):
        result: List[Item] = []
        req_keys: List[Item] = []
        kwargs = {
            'RequestItems': {
                self.name: {
                    'Keys': req_keys,
                    'ConsistentRead': consistent,
                }
            },
            'ReturnConsumedCapacity': 'TOTAL',
        }
        id_vals_iter = iter(id_vals)
        def fill_keys(n: int = 100):
            batch = islice(id_vals_iter, n - len(req_keys))
            req_keys.extend([ self._id_key(id) for id in batch ])
        fill_keys()
        while len(req_keys) > 0 :
            r: dict = self.ddb.batch_get_item(**kwargs)
            print('DDB', r['ConsumedCapacity'])
            all_responses: dict = r.get('Responses', {})
            responses: List[dict] = all_responses.get(self.name, [])
            for item in responses:
                attrs = _unmarshal(item)
                attrs.pop(SK, None)
                pk: str = attrs.pop(PK)
                attrs[self.id_key] = pk.split('#')[-1]
                result.append(attrs)
            all_unprocessed: dict = r.get('UnprocessedKeys', {})
            unprocessed: dict = all_unprocessed.get(self.name, {})
            req_keys.clear()
            req_keys.extend(unprocessed.get('Keys', []))
            fill_keys()
        return result

    def get_sk(self, id_val: str, sk: str, consistent = True):
        r = self.ddb.get_item(
            TableName = self.name,
            Key = {
                PK: _serialize(self._id_pk(id_val)),
                SK: _serialize(sk)
            },
            ConsistentRead = consistent,
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', r['ConsumedCapacity'])
        item = r.get('Item')
        if item is None:
            return None
        attrs = _unmarshal(item)
        attrs.pop(PK, None)
        attrs.pop(SK, None)
        return attrs

    def get_uq(self, uq_key: str, uq_val: str):
        r = self.ddb.query(
            TableName = self.name,
            KeyConditionExpression = f'{PK} = :p',
            ExpressionAttributeValues = {
                ':p': _serialize(self._uq_pk(uq_key, uq_val))
            },
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', r['ConsumedCapacity'])
        uq_item = _unmarshal(r.get('Items')[0])
        id_val = uq_item[self.id_key]
        return self.get(id_val)

    def list_sk(self, id_val: str, sk_prefix: str, consistent = True):
        r: dict = self.ddb.query(
            TableName = self.name,
            KeyConditionExpression = f'{PK} = :p AND begins_with({SK}, :s)',
            ExpressionAttributeValues = {
                ':p': _serialize(self._id_pk(id_val)),
                ':s': _serialize(sk_prefix)
            },
            ConsistentRead = consistent,
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', r['ConsumedCapacity'])
        items: List[Item] = []
        for item in r.get('Items', []):
            attrs = _unmarshal(item)
            attrs.pop(PK, None)
            items.append(attrs)
        return items

    def put(self, attrs: Item):
        put_args = {
            'TableName': self.name,
            'ConditionExpression': f'attribute_not_exists({PK})'
        }
        id_val = attrs[self.id_key]
        items = [
            self._item(attrs),
            *[ self._uq_item(id_val, uq_key, attrs[uq_key]) for uq_key in self.uq_keys ]
        ]
        try:
            res = self.ddb.transact_write_items(
                TransactItems = [ { 'Put': put_args | { 'Item': item } } for item in items ],
                ReturnConsumedCapacity = 'TOTAL',
            )
        except self.ddb.exceptions.TransactionCanceledException as e:
            res = e.response
            print('TransactionCanceledException:', json.dumps(res, default = str))
            err = {}
            reasons = res['CancellationReasons']
            for i, reason in enumerate(reasons):
                code = reason['Code']
                if code == 'None':
                    continue
                if code != 'ConditionalCheckFailed':
                    raise
                if i == 0:
                    err[self.id_key] = f'Item with {self.id_key}={id_val} already exists'
                else:
                    uq_key = self.uq_keys[i - 1]
                    uq_val = attrs[uq_key]
                    err[uq_key] = f'Item with {uq_key}={uq_val} already exists'
            return { 'error': err }
        print('DDB', res['ConsumedCapacity'])
        return { 'items': [ _unmarshal(item) for item in items ] }

    def put_sk(self, id_val: str, sk: str, attrs: Item, ttl: timedelta = None):
        rec = new_record(ttl)
        keys = {
            PK: self._id_pk(id_val),
            SK: sk,
        }
        body = keys | attrs | rec
        res = self.ddb.put_item(
            TableName = self.name,
            Item = _serialize(body)['M'],
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', res['ConsumedCapacity'])
        return rec

    def update(self, id_val: str, attrs: Item):
        exclude = { PK, SK, self.id_key, 'created' }
        _attrs = { k: v for k, v in attrs.items() if k not in exclude } | { 'updated': _now() }
        acts: Dict[str, List[str]] = {}
        names: Dict[str, str] = {}
        values: Dict[str, Item] = {}
        for key, val in _attrs.items():
            names[f'#_{key}'] = key
            if isinstance(val, UpdateAction):
                act = val.clause.value
                exp = val.expression(key)
                val = val.value
            else:
                act = UpdateActionType.SET.value
                exp = f'#_{key} = :_{key}'
            acts.setdefault(act, []).append(exp)
            values[f':_{key}'] = _serialize(val)
        exps = [ f'{k} ' + ', '.join(v) for k, v in acts.items() ]
        print('UPDATE:names', names)
        print('UPDATE:exps', exps)
        print('UPDATE:vals', values)
        res = self.ddb.update_item(
            TableName = self.name,
            Key = self._id_key(id_val),
            UpdateExpression = '\n'.join(exps),
            ExpressionAttributeNames = names,
            ExpressionAttributeValues = values,
            ConditionExpression = f'attribute_exists({PK})',
            ReturnValues = 'UPDATED_OLD', # ALL_NEW, UPDATED_OLD, NONE, ALL_OLD
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', res['ConsumedCapacity'])
        return _unmarshal(res['Attributes'])

    def update_uq(self, id_val: str, uq_key: str, uq_new: str, uq_old: str):
        put_new_value = {
            'Put': {
                'TableName': self.name,
                'Item': self._uq_item(id_val, uq_key, uq_new),
                'ConditionExpression': f'attribute_not_exists({PK})'
            }
        }
        update_owner_item = {
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
        }
        delete_old_value = {
            'Delete': {
                'TableName': self.name,
                'Key': self._uq_item(id_val, uq_key, uq_old),
            }
        }
        res = self.ddb.transact_write_items(
            TransactItems = [
                put_new_value,
                update_owner_item,
                delete_old_value
            ],
            ReturnConsumedCapacity = 'TOTAL',
        )
        print('DDB', res['ConsumedCapacity'])
