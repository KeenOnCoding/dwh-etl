#!/bin/sh

export PYTHONPATH="${PYTHONPATH}:./commons:./processor/nlms_processor"
export PYTHONHASHSEED=0

python3 ./processor/nlms_processor/start.py
