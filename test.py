from asyncio import get_event_loop
from aioazstorage import TableClient
from os import environ
from datetime import datetime
from uuid import uuid1

# TODO: add SAS token support, reference:
# https://github.com/yokawasa/azure-functions-python-samples/blob/master/blob-sas-token-generator/function/run.py

STORAGE_ACCOUNT=environ['STORAGE_ACCOUNT']
STORAGE_KEY=environ['STORAGE_KEY']

async def main():
    t = TableClient(STORAGE_ACCOUNT, STORAGE_KEY)
    #print("Table Deletion", end=" ")
    #print((await t.deleteTable('aiotest')).status)
    print("Table Creation", end=" ")
    print((await t.createTable('aiotest')).status)
    print("Table Query", end=" ")
    async for item in t.getTables({"$filter": "TableName eq 'aiotest'"}):
        print(item['TableName'], end=" ")
    print("\nInsertion", end=" ")
    for i in range(10):
        res = await t.insertEntity('aiotest', {  
            "Address":"Mountain View",
            "Age":23 + i,  
            "AmountDue":200.23,  
            "CustomerCode": str(uuid1()), # send this as string intentionally
            "CustomerSince@odata.type": "Edm.DateTime",  
            "CustomerSince":datetime.now(),
            "IsActive": True,  
            "NumberOfOrders": 255,
            "PartitionKey":"mypartitionkey",  
            "RowKey": "Customer%d" % i
        })
        print(res.status, end=" ")
    print("\nDeletion")
    for i in range(10):
        res = await t.deleteEntity('aiotest', {  
            "PartitionKey":"mypartitionkey",  
            "RowKey": "Customer%d" % i
        })
        print(res.status, end=" ")
    print("\nUpsert")
    for i in range(10):
        res = await t.insertOrReplaceEntity('aiotest', {  
            "Address":"Mountain View",
            "Age": 23 - i,  
            "AmountDue": 0,  
            "CustomerCode": uuid1(), # this updates the entry schema as well
            "CustomerSince@odata.type": "Edm.DateTime",  
            "CustomerSince":datetime.now(),
            "IsActive": True,  
            "NumberOfOrders": 0,
            "PartitionKey":"mypartitionkey",  
            "RowKey": "Customer%d" % i
        })
        print(res.status, end=" ")
    print("\nQuery")
    async for item in t.queryEntities('aiotest', {"$filter": "Age gt 0"}):
        print(item['RowKey'], end= " ")
    print()
    await t.close()

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())