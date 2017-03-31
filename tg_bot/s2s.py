from multiprocessing import Process, Queue
# from multiprocessing.queue import Empty
import time

import telepot
import yaml

from seq2seq.runner import decode


config = yaml.load(open('config.yml').read())
in_msg = Queue()
out_msg = Queue()
chat_id = config['chat_id']
reload_msg = '/reload'


def run_tg(bot):
    bot.handle = handle
    print('I am listening ...')
    bot.message_loop()
    while 1:
        time.sleep(10)


def f(q_to, q_from):
    decode(q_to, q_from)


def work_with_model(bot):
    while 1:
        q_to = Queue()
        q_from = Queue()
        p = Process(target=f, args=(q_to, q_from))
        p.start()
        init = q_from.get()
        bot.sendMessage(chat_id, init)
        while 1:
            message = in_msg.get()
            if message.startswith(reload_msg):
                bot.sendMessage(chat_id, 'Wait a lot.')
                break
            q_to.put(message)
            from_model = q_from.get()
            out_msg.put(from_model)
        p.terminate()


def handle(msg):
    # print(msg)
    if 'chat' not in msg:
        return
    if 'id' not in msg['chat']:
        return
    if msg['chat']['id'] != chat_id:
        return
    if 'text' in msg:
        in_msg.put(msg['text'].lower())
        # print(msg['text'].startswith(reload_msg))
        if not msg['text'].startswith(reload_msg):
            answer = out_msg.get()
            if answer.strip() == '':
                answer = '%NO_MSG%'
            bot.sendMessage(chat_id, answer, reply_to_message_id=msg['message_id'])


# if __name__ == '__main__':
config = yaml.load(open('config.yml').read())
bot = telepot.Bot(config['telegram'])
p = Process(target=run_tg, args=(bot,))
p.start()
work_with_model(bot)
# p.join()
