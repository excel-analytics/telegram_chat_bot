#encoding:utf-8

import os
import random
import csv
from pprint import pprint
import re
import logging

import pymongo
from pymongo import IndexModel, ASCENDING, DESCENDING
from tqdm import tqdm
from transliterate import translit
import pymorphy2
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words


# Logger settings
formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s'
)
logger = logging.getLogger('DP_UBUNTU')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('DP_UBUNTU.log')
# fh.setLevel(logging.DEBUG)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


# Other global vars
EOU = ' __EOU__ '  # End of utterance
EOT = ' __EOT__ '  # End of turn
EOS = ' __EOS__ '  # End of sentence (if was replaced)
TOKENIZER = RegexpTokenizer(r'\w+')
URL_PATTERN = re.compile(r'https?:\/\/[^\s]*')
DIGITS_PATTERN = re.compile(r'\d+')
MORPH = pymorphy2.MorphAnalyzer()
STOPS = get_stop_words('ru')
STOPS.extend(get_stop_words('en'))
MONGO_HAS_SOME_TEXT = {
    '$or': [
        {'$and': [
            {'media.caption': {'$ne': ''}},
            {'media.caption': {'$exists': True}}
        ]},
        {'text': {'$exists': True}}
    ]
}
TIME_DIFF_FOR_SAME_UTTERANCE = 5 * 60  # 5 mins


def generate_context_length():
    # Like in ubuntu original whitepaper
    return random.randint(3, 22)


def get_content_collection(chat_id):
    logger.info('Start copying data.')
    mongo_client = pymongo.MongoClient()
    db = mongo_client['tg_backup']
    # Move chat data to separate collection
    # First of all: create index!
    content_collection = db['content']
    chat_id_index_name = 'chat_id'
    chat_id_index = IndexModel([('chat_id', ASCENDING)], name=chat_id_index_name)
    if chat_id_index_name not in content_collection.index_information():
        content_collection.create_indexes([chat_id_index])
    # Second: create new collection
    tmp_collection = db['tmp_content']
    cursor_for_copy = content_collection.find({'chat_id': chat_id, **MONGO_HAS_SOME_TEXT})
    # Third: move all docs with any text if not already
    if tmp_collection.count() != cursor_for_copy.count():
        tmp_collection.remove()
        copy_bar = tqdm(cursor_for_copy,
            total=cursor_for_copy.count(),
            unit='msg',
            smoothing=0.01,
            desc='Moving data')
        for doc in copy_bar:
            tmp_collection.insert(doc)
        logger.info('Copied successfuly.')
        # Fourth: create new indexes
        msg_date_index_name = 'date_index'
        msg_date_index = IndexModel([('date', DESCENDING)], name=msg_date_index_name)
        if msg_date_index_name not in tmp_collection.index_information():
            tmp_collection.create_indexes([msg_date_index])
    else:
        logger.info('Data already copied.')
    return tmp_collection


def rus_text_prep(text):
    text.lower().replace('\n', EOS)
    text = re.sub(URL_PATTERN, ' ', text)
    text = re.sub(DIGITS_PATTERN, ' ', text)
    tokens = [MORPH.parse(token)[0].normal_form for token in TOKENIZER.tokenize(text)]
    tokens = [token for token in tokens if token not in STOPS]
    return ' '.join(tokens)


def get_text_from_msg(msg):
    # return msg.get('text', msg.get('media', {}).get('caption', ''))
    if 'text' in msg:
        return msg['text']
    if 'media' in msg:
        return msg['media'].get('caption', ' ')
    logger.warning('Passed msg with no text! MSG_ID: {}'.format(msg['_id']))
    return ' '
    

def get_random_response(go_up=True, limit=20):
    pipeline = [
        {'$sample': {'size': 1}}
    ]
    random_msg = list(CC.aggregate(pipeline))[0]
    init_msg_id = random_msg['_id']
    if not go_up:
        limit = 1
    return get_up_current_utterance(init_msg_id, limit)


def reply_context(reply_id, limit=20):
    context = str()
    parrent_msg = {'text': '', 'reply_id': reply_id}
    cnt = 0
    while (parrent_msg is not None and
            cnt < limit):
        cnt += 1
        # Honestly it is not always end of turn, because self reply are allowed.
        context = '{new}{EOT}{cc}'.format(
            cc=context,
            EOT=EOT,
            new=get_text_from_msg(parrent_msg))
        if 'reply_id' not in parrent_msg:
            break
        reply_id = parrent_msg['reply_id']
        parrent_msg = CC.find_one({'_id': reply_id})
    return context


def get_up_current_utterance(msg_id, limit=20):
    current_utterance = str()
    main_msg = CC.find_one({'_id': msg_id})
    main_author = main_msg['from_id']
    prev_msg_date = main_msg['date']
    iterate_over = enumerate(CC.find({
            'date': {'$lte': main_msg['date']}
        }).sort([('date', DESCENDING)]).limit(100))
    for cnt, msg in iterate_over:
        if (msg is None or
            msg['from_id'] != main_author or
            msg['date'] - prev_msg_date > TIME_DIFF_FOR_SAME_UTTERANCE or
            cnt >= limit):
            break
        text = get_text_from_msg(msg)
        current_utterance = '{new}{EOU}{cu}'.format(
            cu=current_utterance,
            EOU=EOU,
            new=text)
    return current_utterance


def get_up_current_context(msg_id, limit=20):
    current_context = str()
    main_msg = CC.find_one({'_id': msg_id})
    main_author = main_msg['from_id']
    author_changed = False
    iterate_over = enumerate(CC.find({
            'date': {'$lte': main_msg['date']}
        }).sort([('date', DESCENDING)]).limit(100))
    for cnt, msg in iterate_over:
        if msg['from_id'] == main_author:
            if not author_changed:
                continue
        else:
            author_changed = True
        if (msg is None or cnt > limit):
            break
        current_context = '{new}{EOU}{cc}'.format(
            cc=current_context,
            EOU=EOU,
            new=get_text_from_msg(msg))
    return current_context


def like_ubuntu(chat_id, folder, only_reply=False):
    chat_id = '$' + chat_id
    train_csv = open(os.path.join(folder, 'train.csv'), 'w')
    fieldnames = ['context', 'response', 'label']
    writer = csv.DictWriter(train_csv,
        fieldnames=fieldnames,
        delimiter='\t',
        quoting=csv.QUOTE_NONE,
        quotechar='')
    writer.writeheader()
    coursor = CC.find().sort([('date', ASCENDING)])
    pbar = tqdm(enumerate(coursor),
        total=coursor.count(),
        smoothing=0.01,
        unit='msg',
        desc='Building dataset')
    logger.info('Start building dataset.')
    for cnt, msg in pbar:
        # if cnt > 5:
        #     break
        logger.debug('MSG #{}.'.format(cnt))
        curr_msg_text = rus_text_prep(get_text_from_msg(msg))
        curr_msg_id = msg['_id']
        context_len = generate_context_length()
        logger.debug('Context len = {}.'.format(context_len))
        if 'reply_id' in msg:
            logger.debug('Reply msg.')
            context = rus_text_prep(reply_context(msg['reply_id'], limit=context_len))
            logger.debug('Context: {}.'.format(context))
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
            writer.writerow(true_obs)
            writer.writerow(false_obs)
        else:
            # noreplay
            if not only_reply:
                logger.debug('Non-reply msg.')
                context = rus_text_prep(get_up_current_context(curr_msg_id, limit=context_len))
                logger.debug('Context: {}.'.format(context))
                true_obs = {
                    'response': rus_text_prep(get_up_current_utterance(curr_msg_id)),
                    'context': context,
                    'label': 1
                }
                false_obs = {
                    'response': rus_text_prep(get_random_response(go_up=True, limit=context_len)),
                    'context': context,
                    'label': 0
                }
                writer.writerow(true_obs)
                writer.writerow(false_obs)
    logger.info('Dataset built.')
    train_csv.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Prepare data for chatbot training in ubuntu-like form.')
    parser.add_argument('--chat_id', type=str, help='Chat id with no `$` sign.')
    parser.add_argument('--folder', type=str, default='data', help='Dir to store data.')
    parser.add_argument('--only_reply', action='store_true', help='If you want to deal only with replies.')
    args = parser.parse_args()
    CC = get_content_collection('$' + args.chat_id)
    like_ubuntu(args.chat_id, args.folder, args.only_reply)
    # python prepare_ubuntu_like_data.py --chat_id 05000000ef461b4143b2772dd6c0a522 --folder tmp_utro
