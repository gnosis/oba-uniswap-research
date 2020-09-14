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

## If uniswap theGraph API changes, then run:
```bash
make
```