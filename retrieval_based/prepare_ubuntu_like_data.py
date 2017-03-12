#encoding:utf-8

import os
import random
import csv
from pprint import pprint
import re

import pymongo
from pymongo import IndexModel, ASCENDING, DESCENDING
from tqdm import tqdm
from transliterate import translit
import pymorphy2
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words


def get_content_collection():
    mongo_client = pymongo.MongoClient()
    db = mongo_client['tg_backup']
    content_collection = db['content']
    chat_id_index_name = 'chat_id'
    chat_id_index = IndexModel([('chat_id', ASCENDING)], name=chat_id_index_name)
    if chat_id_index_name not in content_collection.index_information():
        content_collection.create_indexes([chat_id_index])
    msg_date_index_name = 'date_index'
    msg_date_index = IndexModel([('date', DESCENDING)], name=msg_date_index_name)
    if msg_date_index_name not in content_collection.index_information():
        content_collection.create_indexes([msg_date_index])
    return content_collection


CC = get_content_collection()
EOU = ' __EOU__ '
EOT = ' __EOT__ '
EOS = ' __EOS__ '
TOKENIZER = RegexpTokenizer(r'\w+')
URL_PATTERN = re.compile(r'https?:\/\/[^\s]*')
DIGITS = re.compile(r'\d+')
MORPH = pymorphy2.MorphAnalyzer()
STOPS = get_stop_words('ru')
STOPS.extend(get_stop_words('en'))
MONGO_HAS_SOME_TEXT = {
    '$or': [
        {'$and': [{'media.caption': {'$ne': ''}}, {'media.caption': {'$exists': True}}]},
        {'text': {'$exists': True}}
    ]
}


def rus_text_prep(text):
    text.lower().replace('\n', EOS)
    text = re.sub(URL_PATTERN, ' ', text)
    # text = re.sub(DIGITS, ' ', text)
    tokens = [MORPH.parse(token)[0].normal_form for token in TOKENIZER.tokenize(text)]
    # tokens = [token for token in tokens if token not in STOPS]
    return ' '.join(tokens)


def get_random_response(go_up=True):
    pipeline = [
        {
            '$match': {**MONGO_HAS_SOME_TEXT}
        },
        {
            '$sample': {'size': 1}
        }
    ]
    msg = list(CC.aggregate(pipeline))[0]
    init_msg_id = msg['_id']
    limit = 1
    if go_up:
        limit = 100
    return get_up_current_utterance(init_msg_id, limit)


def reply_context(reply_id):
    context = str()
    parrent_msg = {'text': '', 'reply_id': reply_id}
    while 1:
        if ((parrent_msg is None) or
                ('text' not in parrent_msg) or
                (len(context.split()) > 100)):
            break
        context = '{cc}{EOT}{new}'.format(
            cc=context,
            EOT=EOT,
            new=parrent_msg['text']
        )
        if 'reply_id' not in parrent_msg:
            break
        reply_id = parrent_msg['reply_id']
        parrent_msg = CC.find_one({'_id': reply_id})
    return context


def get_up_current_utterance(msg_id, limit=100):
    current_utterance = str()
    main_msg = CC.find_one({'_id': msg_id})
    main_author = main_msg['from']['id']
    iterate_over = enumerate(CC.find({
        'date': {'$lte': main_msg['date']},
        **MONGO_HAS_SOME_TEXT
    }).sort([('date', DESCENDING)]).limit(100))
    prev_msg_date = main_msg['date']
    for cnt, msg in iterate_over:
        if ((cnt >= limit) or
                (msg is None) or
                (msg['from']['id'] != main_author) or
                (msg['date'] - prev_msg_date < 5 * 60)):
            break
        text = msg.get('text', msg.get('media', {}).get('caption', ''))
        current_utterance = '{cc}{EOU}{new}'.format(
            cc=current_utterance,
            EOU=EOU,
            new=text
        )
    return current_utterance


def get_up_current_context(msg_id, limit=100):
    current_context = str()
    main_msg = CC.find_one({'_id': msg_id})
    main_author = main_msg['from']['id']
    author_changed = False
    iterate_over = enumerate(CC.find({
            'date': {'$lte': main_msg['date']},
            **MONGO_HAS_SOME_TEXT
        }).sort([('date', DESCENDING)]).limit(100)
    )
    for cnt, msg in iterate_over:
        if msg['from']['id'] == main_author:
            if not author_changed:
                continue
        else:
            author_changed = True
        if ((msg is None) or
                (len(current_context.split()) > limit)):
            break
        current_context = '{cc}{EOU}{new}'.format(
            cc=current_context,
            EOU=EOU,
            new=msg.get('text', msg.get('media', {}).get('caption', ''))
        )
    return current_context


def like_ubuntu(chat_id, folder):
    chat_id = '$' + chat_id
    
    coursor = CC.find({
        'chat_id': chat_id,
        **MONGO_HAS_SOME_TEXT
    }).sort([('date', 1)])
    train_csv = open(os.path.join(folder, 'train.csv'), 'w')
    fieldnames = ['context', 'response', 'label']
    writer = csv.DictWriter(train_csv,
        fieldnames=fieldnames,
        delimiter='\t',
        quoting=csv.QUOTE_NONE,
        quotechar='')
    writer.writeheader()
    cnt = 0
    for msg in tqdm(coursor, total=(coursor.count()), smoothing=0.01, unit='msg'):
        # cnt += 1
        # if cnt == 40:
        #     break
        curr_msg_text = rus_text_prep(msg.get('text', msg.get('media', {}).get('caption', '')))
        curr_msg_id = msg['_id']

        if 'reply_id' in msg:
            context = rus_text_prep(reply_context(msg['reply_id']))
            true_obs = {
                'response': curr_msg_text,
                'context': context,
                'label': 1
            }
            false_obs = {
                'response': rus_text_prep(get_random_response(go_up=False)),
                'context': context,
                'label': 0
            }
        else:
            # noreplay
            context = rus_text_prep(get_up_current_context(curr_msg_id))
            true_obs = {
                'response': rus_text_prep(get_up_current_utterance(curr_msg_id)),
                'context': context,
                'label': 1
            }
            false_obs = {
                'response': rus_text_prep(get_random_response(go_up=True)),
                'context': context,
                'label': 0
            }
        writer.writerow(true_obs)
        writer.writerow(false_obs)
    train_csv.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Prepare data for chatbot training.')
    parser.add_argument('--chat_id', type=str)
    parser.add_argument('--folder', type=str)
    args = parser.parse_args()
    like_ubuntu(args.chat_id, args.folder)
    # python prepare_ubuntu_like_data.py --chat_id 05000000ef461b4143b2772dd6c0a522 --folder tmp_utro
