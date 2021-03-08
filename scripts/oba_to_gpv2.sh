#!/bin/bash

# $1 input dir with oba instances
# $2 output dir to store gpv2_instances

for i in $1/*; do python src/oba_from_uniswap/oba_to_gpv2.py $i $2/$(basename $i) --default_fee=0 --exclude_market_makers=1; done
