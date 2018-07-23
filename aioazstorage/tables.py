from aiohttp import ClientSession
from asyncio import sleep
from base64 import b64encode, b64decode
from datetime import datetime
from dateutil import parser
from email.utils import formatdate
from hashlib import sha256, md5
from hmac import HMAC
from time import time, mktime
from urllib.parse import urlencode, quote_plus, quote
from uuid import uuid1, UUID
try:
    from ujson import dumps, loads
except ImportError:
    from json import dumps, loads


_edm_types = {
    datetime: "Edm.DateTime",
    int: "Edm.Int64",
    UUID: "Edm.Guid"
}

_edm_formatters = {
    datetime: lambda d: d.utcnow().isoformat(),
    int: lambda d: str(d),
    UUID: lambda d: str(d)
}

_edm_parsers = {
    "Edm.DateTime": lambda d: parse(d),
    "Edm.Int64": lambda d: int(d),
    "Edm.Guid": lambda d: UUID(d)
}


class TableClient:
    account = None
    auth = None
    session = None

    def __init__(self, account, auth=None, session=None):
        """Create a QueueClient instance"""

        self.account = account
        self.auth = b64decode(auth)
        if session is None:
            session = ClientSession(json_serialize=dumps)
        self.session = session

    async def close(self):
        await self.session.close()

    def _headers(self, date=None):
        """Default headers for REST requests"""

        if not date:
            date = formatdate(usegmt=True) # if you don't use GMT, the API breaks
        return {
            'x-ms-date': date,
            'x-ms-version': '2018-03-28',
            'Content-Type': 'application/json',
            'Accept': 'application/json;odata=nometadata', # we want lean replies for faster handling
            'Prefer': 'return-no-content',
            'x-ms-client-request-id': str(uuid1()), # optional, but useful for debugging
            'Connection': 'Keep-Alive'
        }


    def _sign_for_tables(self, canonicalized, payload=''):
        """Compute SharedKeyLite authorization header and add standard headers"""

        date = formatdate(usegmt=True)
        sign = "\n".join([date, canonicalized]).encode('utf-8')
        return {
            'Authorization': 'SharedKeyLite {}:{}'.format(self.account, \
                b64encode(HMAC(self.auth, sign, sha256).digest()).decode('utf-8')),
            'Content-Length': str(len(payload)),
            **self._headers(date)
        }

    async def getTables(self, query={}):
        """Generator for enumerating tables, with optional OData query"""

        canon = '/{}/Tables'.format(self.account)
        base_uri = 'https://{}.table.core.windows.net/Tables'.format(self.account)
        continuation = 'NextTableName'
        while True:
            if len(query.keys()):
                uri = base_uri + '?' + urlencode(query)
            else:
                uri = base_uri
            async with self.session.get(uri, headers=self._sign_for_tables(canon)) as resp:
                if resp.status == 200:
                    for item in (await resp.json(loads=loads))['value']:
                        yield item
                    cont = resp.headers.get('x-ms-continuation-%s' % continuation, None)
                    if not cont:
                        return
                    else:
                        query[continuation] = cont
                else:
                    return


    async def createTable(self, name):
        """Create a new table"""
        canon = '/{}/Tables'.format(self.account)
        uri = 'https://{}.table.core.windows.net/Tables'.format(self.account)
        payload = dumps({"TableName": name})
        return await self.session.post(uri, headers=self._sign_for_tables(canon, payload), data=payload)


    async def deleteTable(self, name):
        """Delete a table"""
        canon = "/{}/Tables('{}')".format(self.account, name)
        uri = "https://{}.table.core.windows.net/Tables('{}')".format(self.account, name)
        return await self.session.delete(uri, headers=self._sign_for_tables(canon))


    def _annotate_payload(self, payload):
        for k in list(payload.keys()):
            t = type(payload[k])
            if t in _edm_types:
                payload[k+'@odata.type'] = _edm_types[t]
            if t in _edm_formatters:
                payload[k] = _edm_formatters[t](payload[k])
        return payload

    def _parse_payload(self, payload):
        for k in list(payload.keys()):
            edm = payload.get(k+'@odata.type', None)
            if edm in _edm_parsers:
                payload[k] = _edm_parsers[edm](payload[k])
        # remove EDM annotations
        for k in list(payload.keys()):
            if '@odata.type' in k:
                del payload[k]
        return payload



    async def queryEntities(self, table, query={}):
        """Generator for enumerating entities, with optional OData query"""

        canon = '/{}/{}()'.format(self.account, table)
        base_uri = 'https://{}.table.core.windows.net/{}()'.format(self.account, table)
        continuation = ['NextPartitionKey', 'NextRowKey']
        while True:
            if len(query.keys()):
                uri = base_uri + '?' + urlencode(query)
            else:
                uri = base_uri
            async with self.session.get(uri, headers=self._sign_for_tables(canon)) as resp:
                if resp.status == 200:
                    for item in (await resp.json(loads=loads))['value']:
                        yield item
                    cont = {k: resp.headers.get('x-ms-continuation-%s' % k, None) for k in continuation}
                    if not len(list(filter(lambda x: x is not None, cont.values()))):
                        return
                    else:
                        query.update(cont)
                else:
                    return


    async def insertEntity(self, table, entity={}):
        """Create a new entity"""
        canon = '/{}/{}'.format(self.account, table)
        uri = 'https://{}.table.core.windows.net/{}'.format(self.account, table)
        payload = dumps(self._annotate_payload(entity))
        return await self.session.post(uri, headers=self._sign_for_tables(canon, payload), data=payload)


    async def insertOrReplaceEntity(self, table, entity={}):
        """Inserts or Replaces an entity"""
        canon = "/{}/{}(PartitionKey='{}',RowKey='{}')".format(self.account, table, entity['PartitionKey'], entity['RowKey'])
        uri = "https://{}.table.core.windows.net/{}(PartitionKey='{}',RowKey='{}')".format(self.account, table, entity['PartitionKey'], entity['RowKey'])
        payload = dumps(self._annotate_payload(entity))
        return await self.session.put(uri, headers=self._sign_for_tables(canon, payload), data=payload)


    async def updateEntity(self, table, entity={}, etag=None):
        """Update an entity"""
        canon = "/{}/{}(PartitionKey='{}',RowKey='{}')".format(self.account, table, entity['PartitionKey'], entity['RowKey'])
        uri = "https://{}.table.core.windows.net/{}(PartitionKey='{}',RowKey='{}')".format(self.account, table, entity['PartitionKey'], entity['RowKey'])
        payload = dumps(self._annotate_payload(entity))
        headers = {
            'If-Match': '*' if not etag else etag,
            **self._sign_for_tables(canon, payload)
        }
        return await self.session.put(uri, headers=headers, data=payload)


    async def mergeEntity(self, table, entity={}, etag=None):
        """not implemented"""
        raise NotImplementedError("aiohttp does not support MERGE")

    async def insertOrMergeEntity(self, table, entity={}, etag=None):
        """not implemented"""
        raise NotImplementedError("aiohttp does not support MERGE")


    async def deleteEntity(self, table, entity={}, etag=None):
        """Delete an entity"""
        canon = "/{}/{}(PartitionKey='{}',RowKey='{}')".format(self.account, table, entity['PartitionKey'], entity['RowKey'])
        uri = "https://{}.table.core.windows.net/{}(PartitionKey='{}',RowKey='{}')".format(self.account, table, entity['PartitionKey'], entity['RowKey'])
        headers = {
            'If-Match': '*' if not etag else etag,
            **self._sign_for_tables(canon)
        }
        return await self.session.delete(uri, headers=headers)


    async def batchUpdate(self, table, entities=[]):
        """Update a set of entities"""
        canon = "/{}/$batch".format(self.account)
        uri = "https://{}.table.core.windows.net/$batch".format(self.account)
        batch_boundary = '--batch_{}'.format(str(uuid1()))
        changeset_boundary = '--changeset_{}'.format(str(uuid1()))

        changesets = [
            batch_boundary,
            'Content-Type: multipart/mixed; boundary={}'.format(changeset_boundary),
            '',
            changeset_boundary,
        ]
        for entity in entities:
            changesets.extend([
                'Content-Type: application/http',
                'Content-Transfer-Encoding: binary',
                '',
                'POST https://{}.table.core.windows.net/{} HTTP/1.1'.format(self.account, table),
                'Content-Type: application/json',
                'Accept: application/json;odata=nometadata',
                'Prefer: return-no-content',
                '',
                dumps(self._annotate_payload(entity)),
                changeset_boundary
            ])
        changesets.append(batch_boundary)
        payload = '\n'.join(changesets)
        headers = {
            **self._sign_for_tables(canon),
            'Content-Type': 'multipart/mixed; boundary={}'.format(batch_boundary[2:]),
            'Content-Length': str(len(payload)),
            'Accept-Charset': 'UTF-8'
        }
        return await self.session.post(uri, headers=headers, data=payload)