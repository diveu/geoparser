from telegram.ext import Updater, CommandHandler, MessageHandler
from telegram.ext import Filters
from telegram.ext import BaseFilter
import logging
import json
import pandas as pd
import os
import sys

BASE_FILE_PATH = os.path.abspath(os.path.dirname(sys.argv[0])) + '/tmp/{}_{}.geojson'


class FilterReplyToDocument(BaseFilter):
    def filter(self, message):
        if message.reply_to_message and message.reply_to_message.document:
            return True


class FilterPrivateChat(BaseFilter):
    def filter(self, message):
        return message.chat_id > 0


private_chat = FilterPrivateChat()


class DocHandler:
    def __init__(self, message):
        if message.reply_to_message:
            self.Doc_object = message.reply_to_message.document
            self.reply_to_message_id = message.reply_to_message.message_id
        else:
            self.Doc_object = message.document
            self.reply_to_message_id = None

        self.chat_id = message.chat_id
        self.message_id = message.message_id
        self.file_path = BASE_FILE_PATH.format(self.chat_id, self.message_id)
        self.csv_file_path = self.file_path.replace(".geojson", ".csv")
        self.message = message

    def download(self):
        new_file = self.Doc_object.bot.get_file(self.Doc_object.file_id)
        new_file.download(self.file_path)

    def send(self):
        with open(self.csv_file_path, 'rb') as file:
            self.message.reply_document(file, reply_to_message_id=self.reply_to_message_id)

    def remove(self):
        try:
            os.remove(self.file_path)
            os.remove(self.csv_file_path)
        except:
            pass

    def error_happened(self, text):
        self.message.reply_text(text)
        self.remove()

    def process(self):
        try:
            with open(self.file_path) as f:
                data = json.load(f)
            t_object = [{}]
            processed = False
            if 'features' not in data:
                raise ValueError('Empty file')
            for feature in data['features']:
                if 'geometry' not in feature:
                    raise ValueError('not a geometry feature')
                if 'type' not in feature['geometry']:
                    raise ValueError('no type in geometry')
                if feature['geometry']['type'] in t_object[0].keys():
                    t_object[0][feature['geometry']['type']] += 1
                    processed = True
                else:
                    t_object[0][feature['geometry']['type']] = 1
            df = pd.DataFrame(t_object)
            if not processed:
                raise ValueError('Empty output')
            df.to_csv(self.csv_file_path, index=None)
            self.send()
            self.remove()

        except ValueError as err:
            txt = err.args[0]
            print(txt)
            self.error_happened(txt)

    def doc_worker(self):
        self.download()
        self.process()


def on_doc(bot, update):
    logger.info("doc chat")
    DocHandler(update.message).doc_worker()


def hello(bot, update):
    update.message.reply_text(
        'Hello {}'.format(update.message.from_user.first_name))


logging.basicConfig(format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s', level=logging.INFO)

logger = logging.getLogger(__name__)

updater = Updater(token=os.environ.get('TG_TOKEN') or "")

updater.dispatcher.add_handler(CommandHandler('hello', hello))
updater.dispatcher.add_handler(MessageHandler(Filters.document & private_chat, on_doc))

updater.start_polling()
updater.idle()
