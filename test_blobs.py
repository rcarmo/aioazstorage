from aioazstorage import BlobClient
from os import environ
from sys import argv
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


async def containers():
    c = BlobClient(STORAGE_ACCOUNT, STORAGE_KEY)
    #print("Container Deletion", end=" ")
    #print((await c.deleteContainer('aiotest')).status)

    #print("Container Creation", end=" ")
    #print((await c.createContainer('aiotest')).status)

    print("Container Enumeration", end=" ")
    async for container in c.listContainers():
        print(container)
    return

async def blobs():
    c = BlobClient(STORAGE_ACCOUNT, STORAGE_KEY)

    print("\nBlob Upload:")
    tasks = []
    for i in range(OPERATION_COUNT):
        tasks.append(Task(c.putBlob('aiotest', str(i), bytes('hello world\n', 'utf8'))))
    start = time()
    res = await gather(*tasks)
    print("{} operations/s".format(OPERATION_COUNT/(time()-start)))
    print([r.status for r in res])

    return

if __name__ == '__main__':
    loop = get_event_loop()
    entry_point=globals().get(argv[1])
    loop.run_until_complete(entry_point())