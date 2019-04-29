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
from xml.etree import cElementTree
try:
    from ujson import dumps, loads
except ImportError:
    from json import dumps, loads

class BlobClient:
    account = None
    auth = None
    session = None

    def __init__(self, account, auth=None, session=None):
        """Create a BlobClient instance"""

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
            'Connection': 'Keep-Alive'
        }


    def _sign_for_blobs(self, verb, canonicalized, payload=''):
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

    async def createContainer(self, name):
        """Create a new Container"""
        canon = '/{}/{}'.format(self.account, name)
        uri = 'https://{}.blob.core.windows.net/{}?restype=container'.format(self.account, name)
        return await self.session.put(uri, headers=self._sign_for_blobs("PUT", canon))

    async def deleteContainer(self, name):
        canon = '/{}/{}'.format(self.account, name)
        uri = 'https://{}.blob.core.windows.net/{}?restype=container'.format(self.account, name)
        return await self.session.delete(uri, headers=self._sign_for_blobs("DELETE", canon))

    async def listBlobs(self, name):
        canon = '/{}/{}'.format(self.account, name)
        uri = 'https://{}.blob.core.windows.net/{}?restype=container&comp=list'.format(self.account, name)
        res = await self.session.get(uri, headers=self._sign_for_blobs("GET", canon))
        if res.status == 200:
            for msg in cElementTree.fromstring(await res.text()):
                yield {m.tag: m.text for m in msg}

    async def putBlob(self, container, payload):
        """Upload a blob"""
        canon = '/{}/{}/messages'.format(self.account, container)
        base_uri = 'https://{}.blob.core.windows.net/{}/messages'.format(self.account, queue)
        query = {}
        if visibilitytimeout:
            query['visibilitytimeout'] = visibilitytimeout
        if messagettl:
            query['messagettl'] = messagettl
        if len(query.keys()):
            uri = base_uri + '?' + urlencode(query)
        else:
            uri = base_uri
        payload="<QueueMessage><MessageText>{}</MessageText></QueueMessage>".format(payload)
        return await self.session.post(uri, headers=self._sign_for_blobs("POST", canon, payload), data=payload)

   # https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs 