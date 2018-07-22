from asyncio import get_event_loop
from aioazstorage import StorageClient
from os import environ
from datetime import datetime
from uuid import uuid1

# TODO: add SAS token support, reference:
# https://github.com/yokawasa/azure-functions-python-samples/blob/master/blob-sas-token-generator/function/run.py

STORAGE_ACCOUNT=environ['STORAGE_ACCOUNT']
STORAGE_KEY=environ['STORAGE_KEY']

async def main():
    t = StorageClient(STORAGE_ACCOUNT, STORAGE_KEY)
    #print(await t.deleteTable('aiotest'))
    print(await t.createTable('aiotest'))
    async for item in t.getTables({"$filter": "TableName eq 'aiotest'"}):
        print(item)
    for i in range(10):
        res = await t.insertEntity('aiotest', {  
            "Address":"Mountain View",
            "Age":23 + i,  
            "AmountDue":200.23,  
            "CustomerCode": str(uuid1()),
            "CustomerSince@odata.type": "Edm.DateTime",  
            "CustomerSince":datetime.now(),
            "IsActive": True,  
            "NumberOfOrders": 255,
            "PartitionKey":"mypartitionkey",  
            "RowKey": "Customer%d" % i
        })
        print(res)
    await t.close()

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())