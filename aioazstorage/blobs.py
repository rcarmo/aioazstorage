from aiohttp import ClientSession, ClientResponse
from asyncio import sleep
from base64 import b64encode, b64decode
from datetime import datetime
from email.utils import formatdate, parsedate_to_datetime
from hashlib import sha256, md5
from hmac import HMAC
from xml.etree import cElementTree
from typing import Generator
from logging import getLogger, basicConfig
try:
    from ujson import dumps
except ImportError:
    from json import dumps

log = getLogger(__name__)
basicConfig(format = 'time=%(asctime)s loc=%(funcName)s:%(lineno)d msg="%(message)s"',
            level  = environ.get('LOGLEVEL','DEBUG')


class BlobClient:
    account = None
    auth = None
    session = None


    def __init__(self, account, auth=None, session=None) -> None:
        """Create a BlobClient instance"""

        self.account = account
        self.auth = b64decode(auth)
        if session is None:
            session = ClientSession(json_serialize=dumps)
        self.session = session


    async def close(self) -> None:
        await self.session.close()


    def _headers(self, headers={}, date=None) -> dict:
        """Default headers for REST requests"""

        if not date:
            date = formatdate(usegmt=True) # if you don't use GMT, the API breaks
        return {
            'x-ms-date': date,
            'x-ms-version': '2018-03-28',
            'Content-Type': 'application/octet-stream',
            'Connection': 'Keep-Alive',
            **headers
        }


    def _sign_for_blobs(self, verb: str, canonicalized: str, headers={}, payload='') -> dict:
        """Compute SharedKeyLite authorization header and add standard headers"""
        headers = self._headers(headers)
        signing_headers = sorted(filter(lambda x: 'x-ms' in x, headers.keys()))
        canon_headers = "\n".join("{}:{}".format(k, headers[k]) for k in signing_headers)
        sign = "\n".join([verb, '', headers['Content-Type'], '', canon_headers, canonicalized]).encode('utf-8')
        return {
            'Authorization': 'SharedKeyLite {}:{}'.format(self.account, \
                b64encode(HMAC(self.auth, sign, sha256).digest()).decode('utf-8')),
            'Content-Length': str(len(payload)),
            **headers
        }


    async def createContainer(self, container_name) -> ClientResponse:
        canon = f'/{self.account}/{container_name}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container'
        return await self.session.put(uri, headers=self._sign_for_blobs("PUT", canon))


    async def deleteContainer(self, container_name) -> ClientResponse:
        canon = f'/{self.account}/{container_name}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container'
        return await self.session.delete(uri, headers=self._sign_for_blobs("DELETE", canon))


    async def listContainers(self, marker=None) -> Iterator[dict]:
        canon = f'/{self.account}/?comp=list'
        if marker is None:
            uri = f'https://{self.account}.blob.core.windows.net/?comp=list'
        else:
            uri = f'https://{self.account}.blob.core.windows.net/?comp=list&marker={marker}'

        res = await self.session.get(uri, headers=self._sign_for_blobs("GET", canon))
        if res.ok:
            doc = cElementTree.fromstring(res.text.encode('utf-8'))
            for container in doc.findall("//Container"):
                item = {
                    "name": container.find("Name").text
                }
                for prop in container.findall("./Properties/*"):
                    if prop.tag in ["Creation-Time","Last-Modified","Etag","Content-Length","Content-Type","Content-Encoding","Content-MD5","Cache-Control"]:
                        if prop.tag in ["Last-Modified", "DeletedTime"]:
                            item[prop.tag.lower()] = parsedate_to_datetime(prop.text)
                        else:
                            item[prop.tag.lower()] = prop.text
                yield item
            tag = doc.find("NextMarker")
            if tag is not None:
                if tag.text:
                    del res
                    del doc
                    for item in self.listContainers(tag.text):
                        yield item
        else:
            log.error(res.status)
            log.error(await res.text())


    async def listBlobs(self, container_name, marker=None) -> Iterator[dict]:
        canon = f'/{self.account}/{container_name}?comp=list'
        if marker is None:
            uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container&comp=list&include=metadata'
        else:
            uri = f'https://{self.account}.blob.core.windows.net/{container_name}?restype=container&comp=list&include=metadata&marker={marker}'
        res = self.session.get(uri, headers=self._sign_for_blobs("GET", canon))
        if res.ok:
            doc = cElementTree.fromstring(res.text.encode('utf-8'))
            for blob in doc.findall("//Blob"):
                item = {
                    "name": blob.find("Name").text
                }
                for prop in blob.findall("./Properties/*"):
                    if prop.tag in ["AccessTier","Creation-Time","Last-Modified","Etag","Content-Length","Content-Type","Content-Encoding","Content-MD5","Cache-Control"] and prop.text:
                        if prop.tag in ["Last-Modified", "Creation-Time"]:
                            item[prop.tag.lower()] = parsedate_to_datetime(prop.text)
                        elif prop.tag in ["Content-Length"]:
                            item[prop.tag.lower()] = int(prop.text)
                        elif prop.tag in ["Content-MD5"]:
                            item[prop.tag.lower()] = b64decode(prop.text.encode('utf-8'))
                        else:
                            item[prop.tag.lower()] = prop.text
                yield item
            tag = doc.find("NextMarker")
            if tag is not None:
                if tag.text:
                    del res
                    del doc
                    for item in self.listBlobs(container_name, tag.text):
                        yield item
        else:
            log.error(res.status)
            log.error(await res.text())

  
    async def putBlob(self, container_name: str, blob_path: str, payload, mimetype="application/octet-stream") -> ClientResponse:
        """Upload a blob"""
        canon = f'/{self.account}/{container_name}/{blob_path}'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}/{blob_path}'
        headers = {
            'x-ms-blob-type': 'BlockBlob',
            'x-ms-blob-content-type': mimetype,
            'Content-Type': mimetype
        }
        return await self.session.put(uri, data=payload, headers=self._sign_for_blobs("PUT", canon, headers, payload))
        

    async def setBlobTier(self, container_name: str, blob_path: str, tier: str) -> ClientResponse:
        canon = f'/{self.account}/{container_name}/{blob_path}?comp=tier'
        uri = f'https://{self.account}.blob.core.windows.net/{container_name}/{blob_path}?comp=tier'
        headers = {
            'x-ms-access-tier': tier 
        }
        return await self.session.put(uri, headers=self._sign_for_blobs("PUT", canon, headers))
)

   # https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs 
