#!/bin/sh
export PYTHONPATH="${PYTHONPATH}:./commons:./utility/data-lag"
export PYTHONHASHSEED=0

python3 ./utility/data-lag/start.py