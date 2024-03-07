#!/bin/sh -xe
export PYTHONPATH="${PYTHONPATH}:./commons:./utility/dimension-cleanup"
export PYTHONHASHSEED=0

python3 ./utility/dimension-cleanup/start.py