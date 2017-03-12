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


TEST_RATIO = 0.00001


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


def prepare_text(text):
    # return translit('{main}\n'.format(main=text.lower().replace('\n', ' ')), 'ru', reversed=True)
    return '{main}\n'.format(main=text.lower().replace('\n', ' '))


def user_to_user(user_a, user_b, chat_id, filename_mask):
    chat_id = '$' + chat_id
    user_a = '$' + user_a
    user_b = '$' + user_b
    content_collection = get_content_collection()
    coursor_a = content_collection.find({'chat_id': chat_id}).sort([('date', 1)])
    coursor_b = content_collection.find({'chat_id': chat_id}).sort([('date', 1)])
    next(coursor_b)
    train_a = open(filename_mask + '.train.a', 'w')
    train_b = open(filename_mask + '.train.b', 'w')
    test_a = open(filename_mask + '.test.a', 'w')
    test_b = open(filename_mask + '.test.b', 'w')
    for phrase_a, phrase_b in tqdm(zip(coursor_a, coursor_b), total=(coursor_b.count() - 1)):
        # Skip if no text in messages
        if (not 'text' in phrase_a) or (not 'text' in phrase_b):
            continue
        # Skip if a is not user_a or b is not user_b
        if (phrase_a['from']['id'] != user_a) or (phrase_b['from']['id'] != user_b):
            # print(phrase_a['from']['id'], phrase_a['text'])
            # print(phrase_b['from']['id'], phrase_b['text'])
            continue
        prepared_a = prepare_text(phrase_a['text'])
        prepared_b = prepare_text(phrase_b['text'])
        if random.uniform(0, 1) < TEST_RATIO:
            # Then write to test
            test_a.write(prepared_a)
            test_b.write(prepared_b)
        else:
            # Else — write train
            train_a.write(prepared_a)
            train_b.write(prepared_b)
    train_a.close()
    train_b.close()
    test_a.close()
    test_b.close()


def every_to_user(user_b, chat_id, filename_mask):
    chat_id = '$' + chat_id
    user_b = '$' + user_b
    content_collection = get_content_collection()
    coursor_a = content_collection.find({'chat_id': chat_id}).sort([('date', 1)])
    coursor_b = content_collection.find({'chat_id': chat_id}).sort([('date', 1)])
    next(coursor_b)
    train_a = open(filename_mask + '.train.a', 'w')
    train_b = open(filename_mask + '.train.b', 'w')
    test_a = open(filename_mask + '.test.a', 'w')
    test_b = open(filename_mask + '.test.b', 'w')
    for phrase_a, phrase_b in tqdm(zip(coursor_a, coursor_b), total=(coursor_b.count() - 1)):
        # Skip if no text in messages
        if (not 'text' in phrase_a) or (not 'text' in phrase_b):
            continue
        # Skip if a is not user_a or b is not user_b
        if phrase_b['from']['id'] != user_b:
            # print(phrase_a['from']['id'], phrase_a['text'])
            # print(phrase_b['from']['id'], phrase_b['text'])
            continue
        prepared_a = prepare_text(phrase_a['text'])
        prepared_b = prepare_text(phrase_b['text'])
        if random.uniform(0, 1) < TEST_RATIO:
            # Then write to test
            test_a.write(prepared_a)
            test_b.write(prepared_b)
        else:
            # Else — write train
            train_a.write(prepared_a)
            train_b.write(prepared_b)
    train_a.close()
    train_b.close()
    test_a.close()
    test_b.close()


def every_to_every(chat_id, filename_mask):
    def prepare_and_write(f, phrase):
        f.write(prepare_text(phrase))
    def write_train_and_test(train_a, train_b, test_a, test_b, phrase_a, phrase_b):
        if len(phrase_a) == 0 or len(phrase_b) == 0:
            return
        if random.uniform(0, 1) < TEST_RATIO:
            # Then write to test
            prepare_and_write(test_a, phrase_a)
            prepare_and_write(test_b, phrase_b)
        else:
            # Else — write train
            prepare_and_write(train_a, phrase_a)
            prepare_and_write(train_b, phrase_b)

    chat_id = '$' + chat_id
    content_collection = get_content_collection()
    coursor = content_collection.find({'chat_id': chat_id}).sort([('date', 1)])
    train_a = open(filename_mask + '.train.a', 'w')
    train_b = open(filename_mask + '.train.b', 'w')
    test_a = open(filename_mask + '.test.a', 'w')
    test_b = open(filename_mask + '.test.b', 'w')

    prev_bundle_text = str()
    prev_bundle_user = str()

    curr_bundle_text = str()
    curr_bundle_user = str()

    prev_msg_text = str()
    prev_msg_user = str()
    prev_msg_time = 0

    curr_msg_text = str()
    curr_msg_user = str()
    curr_msg_time = 0
    for phrase in tqdm(coursor, total=(coursor.count())):
        # Skip if no text in messages
        if 'text' not in phrase:
            continue
        curr_msg_user = phrase['from']['id']
        curr_msg_time = phrase['date']
        curr_msg_text = phrase['text']
        if curr_msg_user != prev_msg_user:
            # diff users
            if curr_msg_time - prev_msg_time < 3600:
                # like an answer
                if curr_bundle_user != prev_bundle_user:
                    write_train_and_test(train_a, train_b, test_a, test_b, prev_bundle_text, curr_bundle_text) 
                prev_bundle_user = curr_bundle_user
                prev_bundle_text = curr_bundle_text
                curr_bundle_text = curr_msg_text
                curr_bundle_user = curr_msg_user
            else:
                # not like an answer
                if curr_bundle_user != prev_bundle_user:
                    write_train_and_test(train_a, train_b, test_a, test_b, prev_bundle_text, curr_bundle_text) 
                prev_bundle_text = str()
                prev_bundle_user = str()
                curr_bundle_text = curr_msg_text
                curr_bundle_user = curr_msg_user
        else:
            # same users
            if curr_msg_time - prev_msg_time < 5 * 60:
                # like same phrase, but in several messages
                curr_bundle_text = '{curr} {new}'.format(curr=curr_bundle_text, new=curr_msg_text)
                # curr_bundle_user = curr_user
            else:
                # like new message
                if curr_bundle_user != prev_bundle_user:
                    write_train_and_test(train_a, train_b, test_a, test_b, prev_bundle_text, curr_bundle_text)
                prev_bundle_text = str()
                prev_bundle_user = str()
                curr_bundle_text = curr_msg_text
                curr_bundle_user = curr_msg_user

        prev_msg_time = curr_msg_time
        prev_msg_user = curr_msg_user
    train_a.close()
    train_b.close()
    test_a.close()
    test_b.close()


def like_ubuntu(chat_id, folder, debug=False):
    context_word_limit = 100
    EOU = ' __EOU__ '
    EOT = ' __EOT__ '
    EOS = ' __EOS__ '
    TOKENIZER = RegexpTokenizer(r'\w+')
    URL_PATTERN = re.compile(r'https?:\/\/[^\s]*')
    DIGITS = re.compile(r'\d+')
    MORPH = pymorphy2.MorphAnalyzer()
    STOPS = get_stop_words('ru')
    STOPS.extend(get_stop_words('en'))
    def rus_text_prep(text):
        text.lower().replace('\n', EOS)
        text = re.sub(URL_PATTERN, ' ', text)
        # text = re.sub(DIGITS, ' ', text)
        tokens = [MORPH.parse(token)[0].normal_form for token in TOKENIZER.tokenize(text)]
        tokens_no_stops = [token for token in tokens if token not in STOPS]
        return ' '.join(tokens_no_stops)
    def get_random_response(go_up=True):
        pipeline = [
            {
                '$match': {'text': {'$exists': True}}
            },
            {
                '$sample': {'size': 1}
            }
        ]
        msg = list(content_collection.aggregate(pipeline))[0]
        init_msg_id = msg['_id']
        limit = 1
        if go_up:
            limit = 100
        return get_up_current_utterance(init_msg_id, limit)
    def reply_context(reply_id):
        context = str()
        parrent_msg = {'text': '', 'reply_id': reply_id}
        while 1:
            if parrent_msg is None:
                return context
            if 'text' not in parrent_msg:
                return context
            if len(context.split(' ')) > 100:
                return context
            context += EOT + parrent_msg['text']
            if 'reply_id' not in parrent_msg:
                return context
            reply_id = parrent_msg['reply_id']
            parrent_msg = content_collection.find_one({'_id': reply_id})
        return context
    def get_up_current_utterance(msg_id, limit=100):
        current_utterance = str()
        main_msg = content_collection.find_one({'_id': msg_id})
        main_author = main_msg['from']['id']
        for cnt, prev_msg in enumerate(content_collection.find({'date': {'$lte': main_msg['date']}}).sort([('date', -1)]).limit(100)):
            if cnt >= limit:
                return current_utterance
            if prev_msg is None:
                return current_utterance
            if prev_msg['from']['id'] != main_author:
                return current_utterance
            if 'text' not in prev_msg:
                return current_utterance
            current_utterance += prev_msg['text'] + EOU
    def get_up_current_context(msg_id, limit=100):
        current_context = str()
        main_msg = content_collection.find_one({'_id': msg_id})
        main_author = main_msg['from']['id']
        author_changed = False
        iteration_over = enumerate(content_collection.find({
                'date': {'$lte': main_msg['date']},
                'text': {'$exists': True}
            }).sort([('date', DESCENDING)]).limit(100)
        )
        for cnt, prev_msg in iteration_over:
            if prev_msg['from']['id'] == main_author:
                if not author_changed:
                    continue
            else:
                author_changed = True
            if cnt >= limit:
                return current_context
            if prev_msg is None:
                return current_context
            current_context += prev_msg['text'] + EOU
            # print('Here {}'.format(msg_id))
            # pprint(prev_msg)
        return current_context

    chat_id = '$' + chat_id
    content_collection = get_content_collection()
    coursor = content_collection.find({
        'chat_id': chat_id,
        '$or': [
            {'$and': [{'media.caption': {'$ne': ''}}, {'media.caption': {'$exists': True}}]},
            {'text': {'$exists': True}}
        ]
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
    for msg in tqdm(coursor, total=(coursor.count())):
        cnt += 1
        if cnt > 30:
            break
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
            writer.writerow(true_obs)
            writer.writerow(false_obs)
        else:
            # noreplay
            context = rus_text_prep(get_up_current_context(curr_msg_id))
            # print('context =', context)
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
            try:
                writer.writerow(false_obs)
            except:
                pprint(false_obs)
                raise
    train_csv.close()



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Prepare data for chatbot training.')
    parser.add_argument('--chat_id', type=str)
    parser.add_argument('--user_a', type=str)
    parser.add_argument('--user_b', type=str)
    parser.add_argument('--file', type=str)
    args = parser.parse_args()
    # user_to_user(args.user_a, args.user_b, args.chat_id, args.file)
    # python prepare_train_data.py --chat_id 0100000099283b0542dd103c53c7a30c --user_a 01000000e4b71c00209bb3c1ac9e213c --user_b 0100000099283b0542dd103c53c7a30c --file tmp/tr
    # every_to_user(args.user_b, args.chat_id, args.file)
    # python prepare_train_data.py --chat_id 02000000a072bb000000000000000000 --user_a 0 --user_b 0100000006af22038ee504dc096c8596 --file tmp_chpok/
    # every_to_every(args.chat_id, args.file)
    # python prepare_train_data.py --chat_id 05000000ef461b4143b2772dd6c0a522 --user_a 0 --user_b 0 --file tmp_utro/
    like_ubuntu(args.chat_id, args.file, True)
    # python prepare_train_data.py --chat_id 05000000ef461b4143b2772dd6c0a522 --user_a 0 --user_b 0 --file tmp_utro/
