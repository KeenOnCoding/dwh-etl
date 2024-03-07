#!/bin/sh

export PYTHONPATH="${PYTHONPATH}:./commons:./processor/spreadsheet_processor"
export PYTHONHASHSEED=0

python3 ./processor/spreadsheet_processor/start.py
