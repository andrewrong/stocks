from telegram import Bot


class Sender:
    def send(self, rule):
        raise NotImplementedError("Send method not implemented.")


class TelegramSender(Sender):
    def __init__(self, config):
        self.token = config['token']
        self.chat_id = config['chat_id']
        self.bot = Bot(self.token)

    def send(self, msg):
        self.bot.send_message(chat_id=self.chat_id, text=msg)
