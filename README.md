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

## Usage:
```bash
python -m src.main
```

## Creating OBA instances from uniswap trades
```bash
python -m src.oba_from_uniswap.main
```

## If uniswap theGraph API changes, then run:
```bash
make
```