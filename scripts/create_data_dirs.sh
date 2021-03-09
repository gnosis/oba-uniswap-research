#!/bin/bash
# $1 - path prefix (e.g data/oba_from_uniswap/instances-11092424-11098841)
# $2 - n instances (sampled randomly)
# $3 - batch size in seconds
# $4 - t most traded tokens
# $5 - u less frequent user fraction
# $6 - limit price relax fraction

PREFIX=$1/random_sample-$2/s$3-t$4-u$5-l$6
rm -fr $PREFIX
mkdir -p $PREFIX/instances
mkdir -p $PREFIX/solutions
