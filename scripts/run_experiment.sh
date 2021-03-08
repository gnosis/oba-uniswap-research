#!/bin/bash

SOLVER_PATH=~/projects/gnosis/repo/gp-v2-solver
OBA_UNISWAP_PATH=~/projects/gnosis/experimental/oba_uniswap
DATA_PATH=$OBA_UNISWAP_PATH/data/oba_from_uniswap/instances-11827625-11874424

MAX_NR_INSTANCES=200
BATCH_DURATION=60
TOP_TRADED_TOKENS_FRAC=$1
LESS_FREQ_USERS_FRAC=$2
LIMIT_PRICE_RELAX_FRAC=$3

SOLVER_VIRTUAL_ENV_PATH=$SOLVER_PATH/venv
OBA_UNISWAP_VIRTUAL_ENV_PATH=$OBA_UNISWAP_PATH/venv

INSTANCE_PATH=$DATA_PATH/random_sample-$MAX_NR_INSTANCES/s$BATCH_DURATION-t$TOP_TRADED_TOKENS_FRAC-u$LESS_FREQ_USERS_FRAC-l$LIMIT_PRICE_RELAX_FRAC/instances/
SOLUTION_PATH=$DATA_PATH/random_sample-$MAX_NR_INSTANCES/s$BATCH_DURATION-t$TOP_TRADED_TOKENS_FRAC-u$LESS_FREQ_USERS_FRAC-l$LIMIT_PRICE_RELAX_FRAC/solutions/

# Exit on Ctrl-C
trap "exit" INT

# exit when any command fails
set -e

# Create instances

cd $OBA_UNISWAP_PATH
. $OBA_UNISWAP_VIRTUAL_ENV_PATH/bin/activate

./scripts/create_data_dirs.sh $DATA_PATH $MAX_NR_INSTANCES $BATCH_DURATION $TOP_TRADED_TOKENS_FRAC $LESS_FREQ_USERS_FRAC $LIMIT_PRICE_RELAX_FRAC


python -m src.oba_from_uniswap.make_instances \
    $DATA_PATH/per_block.json \
    $INSTANCE_PATH \
    $BATCH_DURATION \
    --max_nr_instances $MAX_NR_INSTANCES \
    --nr_tokens $TOP_TRADED_TOKENS_FRAC \
    --user_fraction $LESS_FREQ_USERS_FRAC \
    --limit_xrate_relax_frac $LIMIT_PRICE_RELAX_FRAC
deactivate

# Run solver

cd $SOLVER_PATH
. $SOLVER_VIRTUAL_ENV_PATH/bin/activate

./scripts/run_many.sh $INSTANCE_PATH $SOLUTION_PATH
mv run_many_last_run_errors.txt $SOLUTION_PATH/../solver_errors.txt || echo "All instances solved successfully."

deactivate

cd $OBA_UNISWAP_PATH
