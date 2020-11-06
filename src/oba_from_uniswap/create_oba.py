# Postprocesses csv files obtained from dune analytics with routed trades

import csv
import argparse
import ast
import json
from tqdm import tqdm
from functools import lru_cache
from frozendict import frozendict
from fractions import Fraction as F
from diskcache import Cache

from ..subgraph import GraphQLClient, UniswapClient

uniswap = UniswapClient()
disk_cache = Cache(directory='.cache')

def load_swaps(filename):
    """Parse a csv file to a list of dicts."""
    r = []
    with open(filename, "r") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            # Skip header.
            if first:
                first = False
                continue
            (block_number, index, sell_amount,
            buy_amount, path, output_amounts, block_time) = row

            path = ast.literal_eval(path)
            path = ['0x' + address for address in path]
            output_amounts = ast.literal_eval(output_amounts.replace('L',''))
            output_amounts = list(map(int, output_amounts))
            r.append({
                'block_number': int(block_number),
                'index': int(index),
                'sell_amount': int(sell_amount),
                'buy_amount': int(buy_amount),
                'path': path,
                'output_amounts': output_amounts,
                'block_time': int(float(block_time))
            })
    return r


def load_tokens(filename):
    with open(filename, "r") as f:
        return json.load(f)


def filter_swaps(swaps, tokens):
    return [
        s for s in swaps
        if {s['path'][0], s['path'][-1]} <= tokens
    ]

def split_by_pair(orders):
    pairs = {(o['sellToken'], o['buyToken']) for o in orders}
    return  {
        pair: [o for o in orders if {o['sellToken'], o['buyToken']} == set(pair)]
        for pair in pairs
    }


# This decorator caches this call parameters and result on disk, so
# that we don't have to wait each time we re-run the code for the same
# list of swaps (happens a lot while developing).
@disk_cache.memoize()
def get_token_info(swaps):
    token_info = {}
    for swap in tqdm(swaps, desc='Querying token info from thegraph'):
        for token_id in swap['path']:
            if token_id in token_info.keys():
                continue
            r = uniswap.get_token(id=token_id)
            token_info[token_id] = {
                'symbol': r.symbol,
                'decimals': r.decimals
            }
    return token_info


# See above comment regarding the decorator.
@disk_cache.memoize()
def get_pool_ids(swaps):
    pool_ids = {}
    for swap in tqdm(swaps, desc='Querying pair IDs from thegraph'):
        for token1, token2 in zip(swap['path'], swap['path'][1:]):
            if (token1, token2) in pool_ids.keys():
                continue
            if (token2, token1) in pool_ids.keys():
                continue
            filter = {
                'token0_in': [token1, token2],
                'token1_in': [token1, token2]
            }
            r = uniswap.get_pairs(filter)
            r = list(r)
            assert len(r) == 1
            pool_ids[r[0].token0.id, r[0].token1.id] = r[0].id
    return pool_ids


# This decorator caches the call in memory, so we don't repeat the same query
# to thegraph.
@lru_cache(maxsize=None)
def get_amm_balances(block_number, token0, token1, pool_ids):
    if (token0, token1) in pool_ids.keys():
        pair_id = pool_ids[token0, token1]
        r = uniswap.get_pair(id=pair_id, block={'number': block_number})
        return r.reserve0, r.reserve1
    else:
        # print((token1, token0))
        assert (token1, token0) in pool_ids.keys()
        pair_id = pool_ids[token1, token0]
        r = uniswap.get_pair(id=pair_id, block={'number': block_number})
        return r.reserve1, r.reserve0

def get_path_amm_balances(block_number, path, pool_ids):
    return [
        get_amm_balances(block_number, token1, token2, pool_ids)
        for token1, token2 in zip(path, path[1:])
    ]


# See above comment regarding the decorator.
@disk_cache.memoize()
def add_amm_balances_to_swaps(swaps, pool_ids):
    for swap in tqdm(swaps, desc='Querying initial AMM balances for each swap'):
        b = get_path_amm_balances(swap['block_number'] - 1, swap['path'], pool_ids)
        swap['amm_balances'] = b
    return swaps


def swap_to_order(swap, token_info):
    order = {}
    amm_path = [token_info[token_id]['symbol'] for token_id in swap['path']]
    amm_amounts = [
        a * 10 ** -int(token_info[t]['decimals'])
        for a, t in zip(swap['output_amounts'], swap['path'])
    ]

    order['sellToken'] = amm_path[0]
    order['buyToken'] = amm_path[-1]

    if swap['sell_amount'] == -1:
        order['maxSellAmount'] = amm_amounts[0]
        min_buy_amount = swap['buy_amount'] * \
            10 ** -int(token_info[swap['path'][-1]]['decimals'])
        xrate = order['maxSellAmount'] / min_buy_amount if min_buy_amount > 0 else float('inf')
        order['limitXRate'] = [
            [min_buy_amount, order['buyToken']],
            [order['maxSellAmount'], order['sellToken']]
        ]
    else:
        assert swap['buy_amount'] == -1
        order['maxBuyAmount'] = amm_amounts[-1]
        max_sell_amount = swap['sell_amount'] * \
            10 ** -int(token_info[swap['path'][0]]['decimals'])
        xrate = max_sell_amount / order['maxBuyAmount'] if order['maxBuyAmount'] > 0 else float('inf')
        order['limitXRate'] = [
            [max_sell_amount, order['sellToken']],
            [order['maxBuyAmount'], order['buyToken']]
        ]

    # xrate as a ratio T1/T2 where T1,T2 are sorted lexicographically
    order['limitXRateCanonical'] = xrate \
        if order['sellToken']<order['buyToken'] else (
            1/xrate if xrate > 0 else float('inf')
        )

    order['fillOrKill'] = True

    order['uniswap'] = {
        'path': amm_path,
        'amounts': amm_amounts,
        'balancesSellToken': [b[0] for b in swap['amm_balances']],
        'balancesBuyToken': [b[1] for b in swap['amm_balances']],
        'block': swap['block_number'],
        'index': swap['index'],
        'timestamp': swap['block_time']
    }
    return order
    
def postprocess(csv_filename, tokens_filename, output_filename):
    token_info = load_tokens(tokens_filename)
    swaps = load_swaps(csv_filename)
    swaps = filter_swaps(swaps, token_info.keys())
    token_info = get_token_info(swaps)  # this gets info also for intermediate tokens
    pool_ids = frozendict(get_pool_ids(swaps))
    swaps = add_amm_balances_to_swaps(swaps, pool_ids)
    orders = [swap_to_order(swap, token_info) for swap in swaps]

    with open(output_filename, 'w+') as f:
        json.dump({
            'orders': orders
        }, f, indent=2)


parser = argparse.ArgumentParser(
    description='Creater an OBA instance file from blockchain data via the csv file generated by '
    'https://explore.duneanalytics.com/queries/9536/source?p_from_block=11093000&p_to_block=11093010#18935'
)
parser.add_argument(
    'filename',
    type=str,
    help='Path to input csv file.'
)

parser.add_argument(
    'token_file',
    type=str,
    help='Path to a json file containing the token addresses to restrict data to.'
)

parser.add_argument(
    'oba_file',
    type=str,
    help='Name of output OBA instance json.'
)

args = parser.parse_args()

postprocess(args.filename, args.token_file, args.oba_file)
