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


class QueueClient:
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
            'Content-Type': 'text/plain; charset=UTF-8',
            'Accept': 'application/json;odata=nometadata',
        }


    def _sign_for_queues(self, verb, canonicalized, payload=''):
        """Compute SharedKeyLite authorization header and add standard headers"""
        headers = self._headers()
        signing_headers = filter(lambda x: 'x-ms' in x, headers.keys())
        canon_headers = "\n".join("{}:{}".format(k, headers[k]) for k in signing_headers)
        sign = "\n".join([verb, '', headers['Content-Type'], '', canon_headers, canonicalized]).encode('utf-8')
        return {
            'Authorization': 'SharedKeyLite {}:{}'.format(self.account, \
                b64encode(HMAC(self.auth, sign, sha256).digest()).decode('utf-8')),
            'Content-Length': str(len(payload)),
            **headers
        }

    async def createQueue(self, name):
        """Create a new queue"""
        canon = '/{}/{}'.format(self.account, name)
        uri = 'https://{}.queue.core.windows.net/{}'.format(self.account, name)
        return await self.session.put(uri, headers=self._sign_for_queues("PUT", canon))


    async def deleteQueue(self, name):
        canon = '/{}/{}'.format(self.account, name)
        uri = 'https://{}.queue.core.windows.net/{}'.format(self.account, name)
        return await self.session.delete(uri, headers=self._sign_for_queues("DELETE", canon))


    def _annotate_payload(self, payload):
        return "<QueueMessage><MessageText>{}</MessageText></QueueMessage>".format(payload)

    def _parse_payload(self, payload):
        pass

    async def putMessage(self, queue, payload, visibilitytimeout=None, messagettl=None):
        """Queue a message"""
        canon = '/{}/{}/messages'.format(self.account, queue)
        base_uri = 'https://{}.queue.core.windows.net/{}/messages'.format(self.account, queue)
        query = {}
        if visibilitytimeout:
            query['visibilitytimeout'] = visibilitytimeout
        if messagettl:
            query['messagettl'] = messagettl
        if len(query.keys()):
            uri = base_uri + '?' + urlencode(query)
        else:
            uri = base_uri
        payload=self._annotate_payload(payload)
        return await self.session.post(uri, headers=self._sign_for_queues("POST", canon, payload), data=payload)
