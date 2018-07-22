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
    from ujson import dumps
except ImportError:
    from json import dumps


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


class StorageClient:
    account = None
    auth = None
    session = None

    def __init__(self, account, auth=None, session=None):
        """Create a StorageClient client"""

        self.account = account
        self.auth = b64decode(auth)
        if session is None:
            session = ClientSession()
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
            'x-ms-client-request-id': str(uuid1()) # optional, but useful for debugging
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
                    for item in (await resp.json())['value']:
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


    async def insertEntity(self, table, entity={}):
        """Create a new entity"""
        canon = '/{}/{}'.format(self.account, table)
        uri = 'https://{}.table.core.windows.net/{}'.format(self.account, table)
        payload = dumps(self._annotate_payload(entity))
        return await self.session.post(uri, headers=self._sign_for_tables(canon, payload), data=payload)