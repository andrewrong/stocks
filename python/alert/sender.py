import asyncio

from telegram import Bot


class Sender:
    async def send(self, rule):
        raise NotImplementedError("Send method not implemented.")

    def sync_send(self, msg):
        raise NotImplementedError("Send method not implemented.")


class TelegramSender(Sender):
    def __init__(self, config):
        self.token = config['token']
        self.chat_id = config['chat_id']
        self.bot = Bot(self.token)

    async def send(self, msg):
        await self.bot.send_message(chat_id=self.chat_id, text=msg)

    def sync_send(self, msg):
        asyncio.run(self.send(msg))
