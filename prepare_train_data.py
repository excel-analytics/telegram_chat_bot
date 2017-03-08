#encoding:utf-8

import random

import pymongo
from tqdm import tqdm
from transliterate import translit


TEST_RATIO = 0.00001


MONGO = pymongo.MongoClient()
DB = MONGO['tg_backup']
CONTENT_COLLECTION = DB['content']


def prepare_text(text):
    # return translit('{main}\n'.format(main=text.lower().replace('\n', ' ')), 'ru', reversed=True)
    return '{main}\n'.format(main=text.lower().replace('\n', ' '))


def user_to_user(user_a, user_b, chat_id, filename_mask):
    chat_id = '$' + chat_id
    user_a = '$' + user_a
    user_b = '$' + user_b
    coursor_a = CONTENT_COLLECTION.find({'chat_id': chat_id}).sort([('date', 1)])
    coursor_b = CONTENT_COLLECTION.find({'chat_id': chat_id}).sort([('date', 1)])
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
    coursor_a = CONTENT_COLLECTION.find({'chat_id': chat_id}).sort([('date', 1)])
    coursor_b = CONTENT_COLLECTION.find({'chat_id': chat_id}).sort([('date', 1)])
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
    coursor = CONTENT_COLLECTION.find({'chat_id': chat_id}).sort([('date', 1)])
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
    every_to_every(args.chat_id, args.file)
    # python prepare_train_data.py --chat_id 0200000066a3db000000000000000000 --user_a 0 --user_b 0
    # python prepare_train_data.py --chat_id 05000000ef461b4143b2772dd6c0a522 --user_a 0 --user_b 0 --file tmp_utro/
