#encoding:utf-8

import random

import pymongo
from tqdm import tqdm
from transliterate import translit


TEST_RATIO = 0.001


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
    every_to_user(args.user_b, args.chat_id, args.file)
    # python prepare_train_data.py --chat_id 02000000a072bb000000000000000000 --user_a 0 --user_b 0100000006af22038ee504dc096c8596 --file tmp_chpok/
