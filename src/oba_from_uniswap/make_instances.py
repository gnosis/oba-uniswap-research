import argparse
import json
import csv
import networkx as nx

def timestamp(o):
    return o['timestamp'] if 'timestamp' in o else o['uniswap']['timestamp']

# Create a pool for every uniswap pool used in any order.
def extract_uniswap_pools(orders):
    pools = {}
    for o in orders:
        if 'uniswap' in o.keys():
            for i in range(len(o['uniswap']['path']) - 1):
                token1 = o['uniswap']['path'][i]
                token2 = o['uniswap']['path'][i + 1]
                balance1 = float(o['uniswap']['balancesSellToken'][i])
                balance2 = float(o['uniswap']['balancesBuyToken'][i])
                pools[token1, token2] = {
                    'token1': token1,
                    'token2': token2,
                    'balance1': balance1,
                    'balance2': balance2,
                    'mandatory': False
                }

    # Remove same pool in inverse directions.
    canonical_pairs = set()
    for k in pools.keys():
        if tuple(reversed(k)) not in canonical_pairs:
            canonical_pairs.add(k)
    return {'-'.join(k): v for k, v in pools.items() if k in canonical_pairs}

# TODO: make more sophisticated as necessary.
def filter_unmatchable_orders(orders, pools):
    edges = {
        (o['sellToken'], o['buyToken']) for o in orders
    } | {
        (p['token1'], p['token2']) for p in pools.values()
    } | {
        (p['token2'], p['token1']) for p in pools.values()
    }
    dg = nx.DiGraph(list(edges))
    return [o for o in orders if nx.has_path(dg, o['buyToken'], o['sellToken'])]

def create_batch(orders):
    pools = extract_uniswap_pools(orders)
    orders = filter_unmatchable_orders(orders, pools)
    orders = {
        id: {
            'sellToken': o['sellToken'],
            'buyToken': o['buyToken'],
            'maxSellAmount': o['maxSellAmount'] if 'maxSellAmount' in o else None,
            'maxBuyAmount': o['maxBuyAmount'] if 'maxBuyAmount' in o else None,
            'limitXRate': o['limitXRate'],
            'fillOrKill': o['fillOrKill']
        }
        for id, o in enumerate(orders)
    }
    return {'orders': orders, 'uniswaps': pools}

# Groups orders in batches.
def batch_iterator(orders, batch_duration):
    nr_orders = len(orders)
    idx = 0
    while idx < len(orders):
        next_idx = next(
            (
                i
                for i in range(idx, nr_orders)
                if timestamp(orders[idx]) + batch_duration < timestamp(orders[i])
            ),
            None
        )
        if next_idx == None:
            next_idx = nr_orders
        yield (idx, next_idx)
        idx = next_idx

def main(oba_file, output_dir, batch_duration=60):
    with open(oba_file, "r") as f:
        oba = json.load(f)['orders']
    for first_idx, last_idx in batch_iterator(oba, batch_duration):
        batch_orders = oba[first_idx:last_idx]
        first_timestamp = timestamp(oba[first_idx])
        last_timestamp = timestamp(oba[last_idx - 1])
        batch = create_batch(batch_orders)
        if len(batch['orders']) == 0:
            continue
        with open(f'{output_dir}/oba_instance_{first_timestamp}-{last_timestamp}.json', 'w+') as f:
            json.dump(batch, f, indent=2)

parser = argparse.ArgumentParser(
    description='Split a large oba instance file into several instances, one for each time interval.')

parser.add_argument(
    'oba_file',
    type=str,
    help='Path to OBA instance file.'
)

parser.add_argument(
    'output_dir',
    type=str,
    help='Path to output directory.'
)

args = parser.parse_args()

main(args.oba_file, args.output_dir)
