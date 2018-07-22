from asyncio import get_event_loop
from aioazstorage import QueueClient
from os import environ
from datetime import datetime
from uuid import uuid1

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
    for i in range(10):
        res = await q.putMessage('aiotest', 'hello world')
        print(res.status, end=" ")
    #print("Queue Deletion", end=" ")
    #print((await t.deleteQueue('aiotest')).status)
    await q.close()

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())