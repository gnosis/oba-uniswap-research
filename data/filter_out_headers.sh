#!/bin/bash
#
# usage:
# cat file1.csv file2.csv | ./filter_out_headers.sh > merged.csv

gawk '($0 !~ /^block_number/)'
