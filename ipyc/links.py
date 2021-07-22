import asyncio
import logging
import sys
import pickle
from typing import Dict, Tuple


SEPARATOR = b'salih'


class MessageObject:
    def __init__(self, function_name: str, message_id: int, *args,  result=None, **kwargs):
        self._function_name = function_name
        self._message_id = message_id
        self._args = args
        self._kwargs = kwargs
        self._result = result
        self._error = False

    @property
    def function_name(self):
        return self._function_name

    @property
    def message_id(self) -> int:
        return self._message_id

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, result):
        self._result = result

    @property
    def args(self) -> Tuple:
        return self._args

    @property
    def kwargs(self) -> Dict:
        return self._kwargs

    @property
    def error(self) -> bool:
        return self._error

    @error.setter
    def error(self, error: bool):
        self._error = error


class AsyncIPyCLink:
    """Represents an abstracted async socket connection that handles
    communication between a :class:`AsyncIPyCHost` and a :class:`AsyncIPyCClient`
    This class is internally managed and typically should not be instantiated on
    its own.

    Parameters
    -----------
    reader: :class:`asyncio.StreamReader`
        The managed inbound data reader.
    writer: :class:`asyncio.StreamWriter`
        The managed outbound data writer
    client: Union[:class:`AsyncIPyCHost`, :class:`AsyncIPyCClient`]
        The communication object that is responsible for managing this connection.
    """
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, client):
        self._reader = reader
        self._writer = writer
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug(f"Established link")
        self._active = True
        self._client = client
        self._tasks: [Dict, asyncio.Future] = {}

    async def close(self):
        """|coro|

        Closes all communication channels with a peer and attempts to send them EOF.
        Informs the parent :class:`AsyncIPyCHost` or :class:`AsyncIPyCClient` of the
        closed connection.
        """
        self._logger.debug(f"Beginning to close link")
        self._reader = None
        if self._writer.can_write_eof():
            self._writer.write_eof()
            try:
                await self._writer.drain()
            except ConnectionAbortedError:
                pass
        self._writer.close()
        if sys.version_info >= (3, 7):
            await self._writer.wait_closed()
        self._writer = None
        self._active = False
        self._client.connections.remove(self)
        self._logger.debug(f"Closed link")

    def is_active(self):
        """:class:`bool`: Indicates if the communication channels are closed, at EOF, or no longer viable."""
        # Quickly check if the state of the reader changed from the remote
        if not self._reader or self._reader.at_eof() or not self._writer:
            self._active = False
        return self._active

    async def send(self, message_object: MessageObject, drain_immediately=True):
        """|coro|

        Send a serializable object to the receiving end. If the object is not a custom
        serializable object, python's builtins will be used. If the object is a custom
        serializable, the receiving end must also have this object in their list of custom
        deserializers.

        .. warning::
            After the result of serialization, either via custom or builtin, the bytes ``0x01`` and ``0x02``
            must not appear anywhere. If your payload does contain these bytes or chars, you must
            substitute them prior to this function call.

        Parameters
        ------------
        message_object: :class:`object`
            The object to be sent to the receiving end.
        drain_immediately: Optional[:class:`bool`]
            Whether to flush the output buffer right now or not.
            Defaults to ``True``.
        """
        if not self.is_active():
            self._logger.debug(f"Attempted to send data when the writer or link is closed! Ignoring.")
            return

        self._writer.write(serialize(message_object))
        if drain_immediately:
            self._logger.debug(f"Draining the writer")
            await self._writer.drain()

        task = asyncio.Future()
        self._tasks[message_object.message_id] = task
        return await task

    async def start_listening(self):
        while self.is_active():
            await self.receive()

    async def receive(self):
        """|coro|

        Receive a serializable object from the other end. If the object is not a custom
        serializable object, python's builtins will be used, otherwise the custom defined
        deserializer will be used.

        Returns
        --------
        Optional[:class:`object`]
            The object that was sent from the sending end. If the deserialization was not successful
            and ``return_on_error`` was set to ``True``, or EOF was encountered resulting in a closed
            connection, ``None`` is returned.
        """
        if not self.is_active():
            self._logger.debug(f"Attempted to read data when the writer or link is closed! Returning nothing.")
            return

        self._logger.debug(f"Waiting for communication from the other side")
        try:
            data = await self._reader.readuntil(separator=SEPARATOR)
        except ConnectionAbortedError:
            self._logger.debug(f"The downstream connection was aborted")
            await self.close()
            return
        except asyncio.exceptions.IncompleteReadError:
            self._logger.debug(f'Read canceled for incomplete read error')
            await self.close()
            return
        message_object = deserialize(data)
        if not isinstance(message_object, MessageObject):
            raise Exception("None Message Object Received")

        if self._reader.at_eof():
            self._logger.debug(f"The downstream writer closed the connection")
            await self.close()
            return None

        if self._client.__class__.__name__ == "AsyncIPyCHost":
            result = None
            try:
                func = getattr(self._client.klass, message_object.function_name)
                result = await func(*message_object.args, **message_object.kwargs) if asyncio.iscoroutinefunction(func)\
                    else func(*message_object.args, **message_object.kwargs)
            except Exception as e:
                result = e
                message_object.error = True
            finally:
                message_object.result = result
            self._writer.write(serialize(message_object))
        else:
            task = self._tasks.pop(message_object.message_id)
            if message_object.error:
                task.set_exception(message_object.result)
            else:
                task.set_result(message_object.result)


def serialize(message_object: MessageObject) -> bytes:
    return pickle.dumps(message_object) + SEPARATOR


def deserialize(data: bytes) -> MessageObject:
    return pickle.loads(data[:-len(SEPARATOR)])
