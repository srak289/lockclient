import os
import time
import asyncio
import logging

from random import randint
from pathlib import Path

class LockClient:
    def __init__(self, name, lockfile, timeout=3):
        self.name = name
        self.__locked = False
        self.__lockfile = lockfile
        self.__lock_timeout = timeout

    @property
    def locked(self):
        return self.__locked

    async def acquire_lock(self):
        try:
            while 1:
                if await self.__read_lock():
                    logging.info(f'{self.name} sees an expired lock')
                    await self.__write_lock()
                    break
                else:
                    logging.info(f'{self.name} awaiting lock')
                    await asyncio.sleep(self.__lock_timeout)
        except FileNotFoundError as e:
            await self.__write_lock()

    async def __read_lock(self):
        '''
        Returns a boolean: True if the lock is expired.
        '''
        with open(self.__lockfile, 'r') as f:
            name, ts = f.read().split('\n')
            if name != self.name:
                self.__locked = False
            return (time.time() - float(ts) > self.__lock_timeout)

    async def __write_lock(self):
        '''
        Write the lock
        '''
        with open(self.__lockfile, 'w') as f:
            f.write(f"{self.name}\n{time.time()}")
        self.__locked = True
        
    async def refresh_lock(self):
        # since we didn't have enough budget for multithreading we made this client async
        # so the driver has the reponsibility to hearbeat the lock
        '''
        Update the lock by calling write_lock.
        '''
        if self.__locked:
            await self.__write_lock()

async def do_work(client):
    logging.info(f"{client.name} is acquiring the lock")
    await client.acquire_lock()

    for x in range(randint(0,5)):
        logging.info(f"{client.name} has the lock and is doing work")
        await asyncio.sleep(1)

    logging.info(f"{client.name} is refreshing the lock")
    await client.refresh_lock()

async def main():
    global lock

    try:
        os.remove(lock)
    except FileNotFoundError as e:
        logging.info("no lock to remove")

    logging.info("setting up")

    logging.info("creating a")
    a = LockClient('client-a', lock)
    logging.info("created a")

    logging.info("creating b")
    b = LockClient('client-b', lock)
    logging.info("created b")

    logging.info("starting loop")
    while 1:
        await asyncio.gather(do_work(a), do_work(b))

cwd = Path(__file__).parent
lock = cwd/'.lockfile'

logging.basicConfig(level=logging.INFO)
try:
    asyncio.run(main())
except Exception as e:
    raise e
finally:
    logging.info("Deleting lockfile")
    os.remove(lock)
