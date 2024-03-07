#!/bin/sh

export PYTHONPATH="${PYTHONPATH}:./commons:./processor/daily_processor"
export PYTHONHASHSEED=0

python3 ./processor/daily_processor/start.py  \
  --env DWH_DB=$DWH_DB \
  --env DWH_USER=$DWH_USER \
  --env DB_PASS=$DB_PASS \
  --env DWH_HOST=$DWH_HOST \
  --env TARGET_TABLE=$TARGET_TABLE