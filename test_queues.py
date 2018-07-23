from aioazstorage import QueueClient
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

# TODO: add SAS token support, reference:
# https://github.com/yokawasa/azure-functions-python-samples/blob/master/blob-sas-token-generator/function/run.py

STORAGE_ACCOUNT=environ['STORAGE_ACCOUNT']
STORAGE_KEY=environ['STORAGE_KEY']
OPERATION_COUNT=int(environ.get('OPERATION_COUNT',100))

async def main():
    q = QueueClient(STORAGE_ACCOUNT, STORAGE_KEY)
    #print("Table Deletion", end=" ")
    #print((await tq.deleteTable('aiotest')).status)
    print("Queue Creation", end=" ")
    print((await q.createQueue('aiotest')).status)
    print("\nInsertion:")
    tasks = []
    for _ in range(OPERATION_COUNT):
        tasks.append(Task(q.putMessage('aiotest', 'hello world')))
    start = time()
    res = await gather(*tasks)
    print("{} operations/s".format(OPERATION_COUNT/(time()-start)))
    #print([r.status for r in res])

    print("Retrieval:")
    receipts = []
    start = time()
    for i in range(int(OPERATION_COUNT/32)+1):
        async for msg in q.getMessages('aiotest', numofmessages=32):
            receipts.append((msg['MessageId'], msg['PopReceipt']))
    print("{} operations/s".format(OPERATION_COUNT/(time()-start)))    
    print("Deletion:")
    tasks = []
    for r in receipts:
        tasks.append(Task(q.deleteMessage('aiotest', *r)))
    start = time()
    res = await gather(*tasks)
    print("{} operations/s".format(OPERATION_COUNT/(time()-start)))
    print()
    await q.close()

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())