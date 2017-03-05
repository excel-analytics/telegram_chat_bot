How to run
``` bash
python ./translate.py  --en_vocab_size=40000 --fr_vocab_size=40000 --data_dir=/home/olive/dev/telegram/telegram_chat_bot --train_dir=/home/olive/dev/telegram/telegram_chat_bot
```


``` bash
cp ../../../../gu.train.a .
cp ../../../../gu.train.b .

python translate.py \
  --data_dir="/home/olive/dev/telegram/telegram_chat_bot" \
  --from_train_data=gu.train.a \
  --to_train_data=gu.train.b \
  --from_dev_data=gu.train.a \
  --to_dev_data=gu.train.b \
  --steps_per_checkpoint=10
```
