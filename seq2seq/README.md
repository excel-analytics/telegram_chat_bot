> Based on [this manual](https://blog.kovalevskyi.com/rnn-based-chatbot-for-6-hours-b847d2d92c43#.p7pxkd6cm).

``` bash
python seq2seq/translate.py \
    --from_vocab_size=50000 \
    --to_vocab_size=50000 \
    --steps_per_checkpoint=100 \
    --data_dir="/home/olive/dev/t/telegram_chat_bot/tmp_hell/" \
    --train_dir="/home/olive/dev/t/telegram_chat_bot/tmp_hell/" \
    --from_train_data="/home/olive/dev/t/telegram_chat_bot/tmp_hell/train.a" \
    --to_train_data="/home/olive/dev/t/telegram_chat_bot/tmp_hell/train.b" \
    --from_dev_data="/home/olive/dev/t/telegram_chat_bot/tmp_hell/train.a" \
    --to_dev_data="/home/olive/dev/t/telegram_chat_bot/tmp_hell/train.b"
```


``` bash
python seq2seq/translate.py \
    --from_vocab_size=50000 --to_vocab_size=50000 \
    --data_dir="/home/olive/dev/t/telegram_chat_bot/tmp_hell" \
    --train_dir="/home/olive/dev/t/telegram_chat_bot/tmp_hell" \
    --decode
```
