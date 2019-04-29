from aioazstorage import BlobClient
from os import environ
from datetime import datetime
from uuid import uuid1
from time import time
from asyncio import set_event_loop_policy, Task, gather
try:
    from uvloop import get_event_loop, EventLoopPolicy
    set_event_loop_policy(EventLoopPolicy())
except ImportError:
    from asyncio import get_event_loop

STORAGE_ACCOUNT=environ['STORAGE_ACCOUNT']
STORAGE_KEY=environ['STORAGE_KEY']
OPERATION_COUNT=int(environ.get('OPERATION_COUNT',100))

async def main():
    c = BlobClient(STORAGE_ACCOUNT, STORAGE_KEY)
    #print("Container Deletion", end=" ")
    #print((await c.deleteContainer('aiotest')).status)
    print("Container Creation", end=" ")
    print((await c.createContainer('aiotest')).status)
    return

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())