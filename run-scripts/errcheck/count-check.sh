#!/bin/sh -xe
export PYTHONPATH="${PYTHONPATH}:./commons:./utility/errcheck"
export PYTHONHASHSEED=0

python3 ./utility/errcheck/count_check.py