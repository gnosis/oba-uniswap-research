#!/bin/bash
# $1 dir to copy from
# $2 copy N files
# $3 dir to copy to

for i in `ls $1 | shuf -n $2`; do cp $1/$i "$3/$(basename $i)"; done
