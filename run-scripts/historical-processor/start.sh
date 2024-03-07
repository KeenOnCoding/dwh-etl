#!/bin/sh

export PYTHONPATH="$PYTHONPATH:./commons:./processor/historical_processor"
export PYTHONHASHSEED=0

python3 ./processor/historical_processor/entrypoint.py
