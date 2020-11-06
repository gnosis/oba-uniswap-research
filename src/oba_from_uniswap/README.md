Set of scripts for creating OBA instances from several data files, namely:

### Uniswap swap files

[CSV files](data/oba_from_uniswap/swaps_data_from_router_11092424-11146424.csv)
containing the swap transactions sent to the uniswap router contract.

This file can be obtained from the blockchain via DuneAnalytics, using
[this query](https://explore.duneanalytics.com/queries/9536/source?p_from_block=11093000&p_to_block=11093010#18935).

### SCP files

[CSV files](data/oba_from_uniswap/scp-crawled_11092424-11144990.csv) containing SCP liquidity 
provision orders.

This file can be obtained from a docker container running on kubernetes:

```bash
kubectl apply -f volume-access.yaml -- sh
kubectl cp multitool-alpine:/data/crawled.csv crawled.csv
kubectl delete pod multitool-alpine
```

Get that `volume-access.yaml` file from 
[here](https://gnosisinc.slack.com/archives/C6Z2XNL5Q/p1603191198190200?thread_ts=1602758167.147100&cid=C6Z2XNL5Q).

# Usage

# Create a raw OBA instance file. 

```bash
python -m src.oba_from_uniswap.create_oba swaps_data_from_router.csv tokens.json oba_raw.json
```

This will create a single json file with all the uniswap swaps, and information about the state of the uniswap 
pools at the time of each swap.

# Merge SCP orders in the raw OBA instance file.

```bash
python -m src.oba_from_uniswap.add_scp_orders oba_raw.json scp-crawled.csv oba_raw_with_scp.json
```

# Create OBA instance files from raw OBA instance file.

Create one OBA instance file for each minute of trading in the raw OBA instance file.

```bash
python -m src.oba_from_uniswap.make_instances oba_raw_with_scp.json instances/
```
