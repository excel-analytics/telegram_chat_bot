import os
import time
import itertools
import sys
import numpy as np

import tensorflow as tf

import udc_model
import udc_hparams
import udc_metrics
import udc_inputs
from dual_encoder import dual_encoder_model
from helpers import load_vocab


tf.flags.DEFINE_string('model_dir', None, 'Directory to load model checkpoints from')
tf.flags.DEFINE_string('vocab_processor_file', './data/vocab_processor.bin', 'Saved vocabulary processor file')
FLAGS = tf.flags.FLAGS


if not FLAGS.model_dir:
    print('You must specify a model directory')
    sys.exit(1)


def tokenizer_fn(iterator):
    return (x.split(' ') for x in iterator)


# Load vocabulary
vp = tf.contrib.learn.preprocessing.VocabularyProcessor.restore(
    FLAGS.vocab_processor_file
)


# Load your own data here
INPUT_CONTEXT = [
    'а здесь слово да глубоко в середине, мы вам его не покажем и вообще пора забыть о неём, что за бред в конце концов. вот',
    'куча текста про всякую фигнню непонютную да',
    'здесь нет, совсем нет заветного слова из двух букв, только хуй'
]
POTENTIAL_RESPONSES = ['ответ со словом хуй', 'пизда']


def get_features(context, utterance):
    context_matrix = np.array(list(vp.transform([context])))
    utterance_matrix = np.array(list(vp.transform([utterance])))
    context_len = len(context.split(' '))
    utterance_len = len(utterance.split(' '))
    features = {
        'context': tf.convert_to_tensor(context_matrix, dtype=tf.int64),
        'context_len': tf.constant(context_len, shape=[1,1], dtype=tf.int64),
        'utterance': tf.convert_to_tensor(utterance_matrix, dtype=tf.int64),
        'utterance_len': tf.constant(utterance_len, shape=[1,1], dtype=tf.int64),
    }
    return features, None


if __name__ == '__main__':
    hparams = udc_hparams.create_hparams()
    model_fn = udc_model.create_model_fn(hparams, model_impl=dual_encoder_model)
    estimator = tf.contrib.learn.Estimator(model_fn=model_fn, model_dir=FLAGS.model_dir)

    # Ugly hack, seems to be a bug in Tensorflow
    # estimator.predict doesn't work without this line
    # estimator._targets_info = tf.contrib.learn.estimators.tensor_signature.TensorSignature(tf.constant(0, shape=[1,1]))
    for c_index, context in enumerate(INPUT_CONTEXT):
        print('Context #{n}: {context}'.format(
            context=context,
            n=c_index
        ))
        for t_index, r in enumerate(POTENTIAL_RESPONSES):
            # prob = estimator.predict(input_fn=lambda: get_features(INPUT_CONTEXT, POTENTIAL_RESPONSES))
            prob = estimator.predict(input_fn=lambda: get_features(context, r))
            prob = next(prob)
            # print(prob)
            # print('{}: {:g}'.format(r, prob[0,0]))
            print('{n}. {response}: {prob:g}'.format(
                n=t_index,
                response=r,
                prob=prob[0]
            ))