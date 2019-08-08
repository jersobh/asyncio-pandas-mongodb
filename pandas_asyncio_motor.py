import sys
import pandas as pd
import json
import os
import time
import asyncio
import time
import motor.motor_asyncio


async def import_content(filepath):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.big_csv
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)
    data = pd.read_csv(file_res)
    data_json = json.loads(data.to_json(orient='records'))
    await db.test_collection.insert_many(data_json)

if __name__ == "__main__":
    start = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(import_content('file.csv'))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        end = time.time()
        print(f'Finished in {end - start} seconds')



