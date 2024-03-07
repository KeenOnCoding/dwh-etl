#!/bin/sh

export PYTHONPATH="$PYTHONPATH:./commons:./processor/sql_processor"
export PYTHONHASHSEED=0

python3 ./processor/sql_processor/start.py
