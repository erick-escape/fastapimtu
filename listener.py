import asyncio
from sender import Sender
from aio_pika import connect, IncomingMessage
from main import dtm
import json

sender = Sender()


async def on_message(message: IncomingMessage):
    info = message.body.decode("utf-8")
    await dtm(info)

if __name__ == "__main__":
    print("listening...")
    loop = asyncio.get_event_loop()
    loop.create_task(sender.consume_messages(on_message))
    loop.run_forever()
