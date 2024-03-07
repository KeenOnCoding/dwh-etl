#!/bin/sh

export PYTHONPATH="${PYTHONPATH}:./commons:./processor/work_experience_processor"
export PYTHONHASHSEED=0

python3 ./processor/work_experience_processor/start.py
