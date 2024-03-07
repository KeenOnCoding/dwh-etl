#!/bin/sh

export PYTHONPATH="$PYTHONPATH:./commons:./ingestion"
export PYTHONHASHSEED=0

python3 ./ingestion/start.py
