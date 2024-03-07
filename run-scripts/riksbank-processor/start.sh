#!/bin/sh

export PYTHONPATH="${PYTHONPATH}:./commons:./processor/riksbank_processor"
export PYTHONHASHSEED=0

python3 ./processor/riksbank_processor/start.py
