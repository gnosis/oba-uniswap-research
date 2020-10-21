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

## If uniswap theGraph API changes, then run:
```bash
make
```