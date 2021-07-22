import logging
import datetime

from ipyc import AsyncIPyCHost, AsyncIPyCLink

# logging.basicConfig(level=logging.DEBUG)


class HelloTest:
    def call_me(self, *args, **kwargs):
        print("you called me", args, kwargs)

    async def async_call_me(self):
        print("you called me asyncly")
        return "I am ok"


hello_test = HelloTest()
host = AsyncIPyCHost(klass=hello_test)


print('Starting to wait for connections!')
host.run()
