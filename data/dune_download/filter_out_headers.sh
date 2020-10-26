#!/bin/bash
#
# usage:
# cd data/dune_download
# cat $(ls | grep "swaps")| ./filter_out_headers.sh > merged.csv

gawk '($0 !~ /^block_number/)'
