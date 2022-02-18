This repo contains a set of scripts for collecting uniswap data and exploratory
research on OBA ideas.

## Create and activate virtual env:
```bash
virtualenv --python /usr/bin/python3 venv
. venv/bin/activate
```

## Install dependencies:
```bash
pip install -r requirements.txt
```

## Buffer Research:

Create a .env with the example from .env.example
Then run:
```
source .env
python3 -m src.buffer_research
```

In order to estimate the revenue from internal buffer trades, its better to run this analysis:

```
source .env
python3 -m src.impermanent_loss_analysis_buffer_trading
```

## Create Data:
To work with the Dune data:
```
cd data/dune_download
cat $(ls | grep "swaps")| ./filter_out_headers.sh > merged.csv
```
The data from thegraph does not be prepared, though the first download might take some time.


## Usage:

In order to run the calculations, run:
```bash
python -m src.probability_of_match_assuming_statistical_dependence
```
or
```bash
python -m src.expected_value
```
or
```bash
python -m src.probability_of_match
```

## Dataset:

In order to adjust the dataset for the calculation, modify the following parameters in download_swaps:

```bash
end_block // this is the last ethereum block to be considered
investigation_period // this is the investigation period in secs
```

## Creating OBA instances from uniswap trades

See [README](oba_from_uniswap/README.md) .

## If uniswap theGraph API changes, then run:
```bash
make
```