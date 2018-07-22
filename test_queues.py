from aioazstorage import QueueClient
from os import environ
from datetime import datetime
from uuid import uuid1
try:
    from uvloop import get_event_loop
except ImportError:
    from asyncio import get_event_loop

# TODO: add SAS token support, reference:
# https://github.com/yokawasa/azure-functions-python-samples/blob/master/blob-sas-token-generator/function/run.py

STORAGE_ACCOUNT=environ['STORAGE_ACCOUNT']
STORAGE_KEY=environ['STORAGE_KEY']

async def main():
    q = QueueClient(STORAGE_ACCOUNT, STORAGE_KEY)
    #print("Table Deletion", end=" ")
    #print((await tq.deleteTable('aiotest')).status)
    print("Queue Creation", end=" ")
    print((await q.createQueue('aiotest')).status)
    print("\nInsertion", end=" ")
    for i in range(100):
        res = await q.putMessage('aiotest', 'hello world')
        print(res.status, end=" ")
    print("\nRetrieval", end=" ")
    receipts = []
    for i in range(100):
        async for msg in q.getMessages('aiotest', numofmessages=2):
            receipts.append((msg['MessageId'], msg['PopReceipt']))
            print(msg['MessageText'], end=" ")
    print("\nDeletion", end=" ")
    for r in receipts:
        res = await q.deleteMessage('aiotest', *r)
        print(res.status, end=" ")
    print()
    await q.close()

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())