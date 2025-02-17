import logging
import asyncio
import time

from ipyc import AsyncIPyCClient

logging.basicConfig(level=logging.DEBUG)


async def hello_world():
    sleep_time = 1
    client = AsyncIPyCClient()
    await client.connect()
    print(f'Connecting to the host...', end=' ')
    print(f'connected!\nSending "Hello World!"...', end=' ')
    jobs = [client.proxy.async_call_me() for _ in range(1)]
    t = time.time()
    res = await asyncio.gather(*jobs)
    print(f'it took {time.time() -t:.2f} seconds')
    print(res)
    sleep_time *= 2
    print(f'sleeping {sleep_time} seconds')
    await asyncio.sleep(sleep_time)
    #print(res)
    print(f'sent!\nClosing connection...', end=' ')
    await client.close()
    print(f'closed!')

loop = asyncio.get_event_loop()
loop.run_until_complete(hello_world())
