How to run
``` bash
python ./translate.py  --en_vocab_size=40000 --fr_vocab_size=40000 --data_dir=/home/olive/dev/telegram/telegram_chat_bot --train_dir=/home/olive/dev/telegram/telegram_chat_bot
```


``` bash
cp ../../../../gu.train.a .
cp ../../../../gu.train.b .

python translate.py \
  --en_vocab_size=40000 \
  --fr_vocab_size=40000 \
  --data_dir="/home/olive/dev/t/telegram_chat_bot/tmp" \
  --train_dir="/home/olive/dev/t/telegram_chat_bot/tmp" \
  --from_train_data="/home/olive/dev/t/telegram_chat_bot/tmp/tr.train.a" \
  --to_train_data="/home/olive/dev/t/telegram_chat_bot/tmp/tr.train.b" \
  --from_dev_data="/home/olive/dev/t/telegram_chat_bot/tmp/tr.train.a" \
  --to_dev_data="/home/olive/dev/t/telegram_chat_bot/tmp/tr.train.b" \
  --steps_per_checkpoint=10


python ./translate.py \
    --en_vocab_size=40000 --fr_vocab_size=40000 \
    --data_dir="/home/olive/dev/t/telegram_chat_bot/tmp" \
    --train_dir="/home/olive/dev/t/telegram_chat_bot/tmp" \
    --decode
```


``` bash
python translate.py \
  --en_vocab_size=40000 \
  --fr_vocab_size=40000 \
  --data_dir="/home/olive/dev/t/telegram_chat_bot/tmp_chpok" \
  --train_dir="/home/olive/dev/t/telegram_chat_bot/tmp_chpok" \
  --from_train_data="/home/olive/dev/t/telegram_chat_bot/tmp_chpok/.train.a" \
  --to_train_data="/home/olive/dev/t/telegram_chat_bot/tmp_chpok/.train.b" \
  --from_dev_data="/home/olive/dev/t/telegram_chat_bot/tmp_chpok/.train.a" \
  --to_dev_data="/home/olive/dev/t/telegram_chat_bot/tmp_chpok/.train.b" \
  --steps_per_checkpoint=10


python ./translate.py \
    --en_vocab_size=40000 --fr_vocab_size=40000 \
    --data_dir="/home/olive/dev/t/telegram_chat_bot/tmp_chpok" \
    --train_dir="/home/olive/dev/t/telegram_chat_bot/tmp_chpok" \
    --decode
```
