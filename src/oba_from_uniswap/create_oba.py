# Postprocesses csv files obtained from dune analytics with routed trades

import argparse
import ast
import csv
import json
from datetime import date, datetime, time, timezone
from fractions import Fraction as F
from functools import lru_cache

from diskcache import Index
from frozendict import frozendict
from networkx.algorithms.shortest_paths.generic import shortest_path, has_path
from tqdm import tqdm
from ..dune_query import run_dune_query

from ..subgraph import GraphQLClient, UniswapClient, UnrecoverableError
from networkx import Graph
from .common import get_largest_element_sequence

# Occasionally users specify market orders on uniswap

# Orders are considered market orders if their limit price distance
# to the obtained price is greater than
IS_MARKET_ORDER_TOL = 0 #.001

# Fix market orders so that they are at this distance from the obtained price
LIMIT_PRICE_TOL = 0.1



uniswap = UniswapClient()
disk_cache = Index(".cache")

print(f"Cache size: {len(disk_cache)}")


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
            buy_amount, path, output_amounts, block_time, address) = row

            path = path.replace("{", "[").replace("}", "]")
            path = ast.literal_eval(path)
            path = ['0x' + address[2:] for address in path]
            output_amounts = output_amounts.replace("{", "[").replace("}", "]")
            output_amounts = ast.literal_eval(output_amounts.replace('L',''))
            output_amounts = list(map(int, output_amounts))
            if path[0]==path[-1]:   # this occasionally happens for some reason
                continue
            r.append({
                'block_number': int(block_number),
                'index': int(index),
                'sell_amount': int(sell_amount),
                'buy_amount': int(buy_amount),
                'path': path,
                'output_amounts': output_amounts,
                'block_time': int(float(block_time)),
                'address': address
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

# Assumes dune query returns swaps ordered by (block, index)
# Removes all swaps with duplicate entries since data from input
# csv might be inaccurate for all of them.
def remove_duplicate_swaps_in_same_block_index(swaps):
    if len(swaps) == 0:
        return swaps
    block_index_to_remove = set()
    for i in range(1, len(swaps)):
        if swaps[i]['block_number'] == swaps[i - 1]['block_number'] and \
           swaps[i]['index'] == swaps[i - 1]['index']:
            block_index_to_remove.add((swaps[i]['block_number'], swaps[i]['index']))
    r = []
    for i in range(len(swaps)):
        if (swaps[i]['block_number'], swaps[i]['index']) in block_index_to_remove:
            continue
        r.append(swaps[i])
    return r


# This decorator caches this call parameters and result on disk, so
# that we don't have to wait each time we re-run the code for the same
# list of swaps (e.g. while developing).
@disk_cache.memoize()
def get_token_info(token_id):
    t = uniswap.get_token(id=token_id)
    return {
        'symbol': t.symbol,
        'decimals': t.decimals
    }

# This decorator caches this call parameters and result on disk, so
# that we don't have to wait each time we re-run the code for the same
# list of swaps (e.g. while developing).
def get_token_infos(swaps):
    token_info = {}
    token_ids = {token_id for swap in swaps for token_id in swap['path']}
    for token_id in tqdm(token_ids, desc='Querying token info from thegraph'):
        r = get_token_info(str(token_id))
        token_info[token_id] = r
    return token_info


# See above comment regarding the decorator.
@disk_cache.memoize()
def get_pool_ids(token1, token2):
    r = uniswap.get_pair_ids(token1, token2)
    return (str(r.token0.id), str(r.token1.id), str(r.id))

def get_pools_ids(swaps):
    pool_ids = {}
    token_pairs = {
        (token1, token2)
        for swap in swaps
        for token1, token2 in zip(swap['path'][:-1], swap['path'][1:])
    }
    for (token1, token2) in tqdm(token_pairs, desc='Querying pair IDs from thegraph'):
        if (token1, token2) in pool_ids.keys():
            continue
        if (token2, token1) in pool_ids.keys():
            continue
        token1_id, token2_id, pool_id = get_pool_ids(token1, token2)
        pool_ids[token1_id, token2_id] = pool_id
    return pool_ids

def get_all_pool_ids():
    pool_ids = {}
    for p in tqdm(uniswap.get_pairs(), desc="Querying all pair IDs from the graph"):
        pool_ids[p.token0.id, p.token1.id] = p.id
        for t1, t2 in [(p.token0.id, p.token1.id), (p.token1.id, p.token0.id)]:
            key = get_pool_ids.__cache_key__(t1, t2)
            disk_cache[key] = (str(p.token0.id), str(p.token1.id), str(p.id))
    return pool_ids

# See above comment regarding the decorator.
@disk_cache.memoize()
def get_pair_reserves(pair_id, block_number):
    r = uniswap.get_pair_reserves(id=pair_id, block={'number': block_number})
    return r.reserve0, r.reserve1

def get_amm_balances(block_number, token0, token1, pool_ids):
    if (token0, token1) in pool_ids.keys():
        pair_id = pool_ids[token0, token1]
        reserve0, reserve1 = get_pair_reserves(str(pair_id), block_number)
    else:
        assert (token1, token0) in pool_ids.keys()
        pair_id = pool_ids[token1, token0]
        reserve1, reserve0 = get_pair_reserves(str(pair_id), block_number)
    return float(reserve0), float(reserve1)

def get_path_amm_balances(block_number, path, pool_ids):
    return [
        get_amm_balances(block_number, token1, token2, pool_ids)
        for token1, token2 in zip(path[:-1], path[1:])
    ]


def add_amm_balances_to_swap(swap, pool_ids):
    b = get_path_amm_balances(swap['block_number'] - 1, swap['path'], pool_ids)
    swap['amm_balances'] = b
    return swap

def add_amm_balances_to_swaps_through_thegraph(swaps, pool_ids):
    r = []
    for swap in tqdm(swaps, desc='Querying initial AMM balances for each swap from TheGraph'):
        try:
            add_amm_balances_to_swap(swap, pool_ids)
            r.append(swap)
        except UnrecoverableError:
            print("Couldn't get reserves for swap. Skipping.")
            pass
    return r

@disk_cache.memoize()
def get_amm_balances_page_from_dune_2(from_block, to_block, contract_address, page_size):
    DUNE_UNISWAP_RESERVES_QUERY_ID = 20482
    rows = run_dune_query(
        DUNE_UNISWAP_RESERVES_QUERY_ID,
        {
            'from_block': from_block,
            'to_block': to_block + 1,
            'contract_address': "\\" + str(contract_address[1:]),
            'page_size': page_size
        }
    )['query_result']['data']['rows']
    return [r for r in rows if r['evt_block_number'] <= to_block]

def get_amm_balances_from_dune(from_block, to_block, pool_id):
    rows = []
    page_size = 1000
    while from_block <= to_block:
        page = get_amm_balances_page_from_dune_2(from_block, to_block, pool_id, page_size)
        if len(page) == 0:
            break
        rows += page
        from_block = page[-1]['evt_block_number'] + 1
    return rows

def get_reserves_from_dune(from_block, to_block, pool_ids, token_info):
    reserves = {}
    for pair_ids, pool_id in tqdm(pool_ids.items(), desc='Querying initial AMM balances for each swap from Dune'):
        token0, token1 = pair_ids
        pool_reserves = {}
        # get initial reserve through thegraph
        try:
            init_reserve0, init_reserve1 = get_amm_balances(from_block, token0, token1, pool_ids)
        except UnrecoverableError:
            print("Couldn't get reserves for token pair. Skipping.")
            continue
        if init_reserve0 == 0 or init_reserve1 == 0:
            continue
        pool_reserves[from_block] = (init_reserve0, init_reserve1)
        # get following reserves through thegraph
        rows = get_amm_balances_from_dune(from_block + 1, to_block, pool_id)
        for row in rows:
            reserve0 = row['reserve0'] * 10 ** -int(token_info[token0]['decimals'])
            reserve1 = row['reserve1'] * 10 ** -int(token_info[token1]['decimals'])
            if reserve0 == 0 or reserve1 == 0:
                continue
            pool_reserves[row['evt_block_number']] = (reserve0, reserve1)
        reserves[pair_ids] = pool_reserves
    return reserves


def get_path_reserves_at_block(path, reserves, pool_ids, block):
    path_reserves = []
    for token0, token1 in zip(path[:-1], path[1:]):
        if (token0, token1) in pool_ids.keys():
            if (token0, token1) not in reserves.keys() or \
                block not in reserves[token0, token1].keys():
                return []
            reserve0, reserve1 = reserves[token0, token1][block]
        else:
            assert (token1, token0) in pool_ids.keys()
            if (token1, token0) not in reserves.keys() or \
                block not in reserves[token1, token0].keys():
                return []
            reserve1, reserve0 = reserves[token1, token0][block]
        path_reserves.append((reserve0, reserve1))
    return path_reserves

def compute_reserves_for_blocks(blocks, reserves):
    reserves_for_blocks = {}
    for k, v in tqdm(reserves.items(), "Computing reserves for blocks"):
        reserve_blocks = get_largest_element_sequence(
            blocks, list(v.keys()), lambda a, b: b <= a
        )
        reserves_for_blocks[k] = {b: v[rb] for b, rb in zip(blocks, reserve_blocks)}
    return reserves_for_blocks

def compute_reserves_for_swaps(swaps, reserves):
    swap_prev_blocks = [s['block_number'] - 1 for s in swaps]
    return compute_reserves_for_blocks(swap_prev_blocks, reserves)

def add_amm_balances_to_swaps_through_dune(swaps, pool_ids, token_info):
    from_block = min(s['block_number'] for s in swaps) - 1
    to_block = max(s['block_number'] for s in swaps)
    reserves = get_reserves_from_dune(from_block, to_block, pool_ids, token_info)
    reserves = compute_reserves_for_swaps(swaps, reserves)    

    r = []
    for swap in tqdm(swaps, "Adding reserves to swaps"):
        path = swap['path']
        prev_block = swap['block_number'] - 1
        path_reserves = get_path_reserves_at_block(path, reserves, pool_ids, prev_block)
        if len(path_reserves) == 0:
            continue
        swap['amm_balances'] = path_reserves
        r.append(swap)
    return r

#add_amm_balances_to_swaps = add_amm_balances_to_swaps_through_dune
add_amm_balances_to_swaps = add_amm_balances_to_swaps_through_thegraph

# See above comment regarding the decorator.
@disk_cache.memoize()
def get_token_day_price(token, date):
    return float(uniswap.get_token_day_price(token, date))

def get_day_start(time_unix):
    return int(
        datetime.combine(
            datetime.utcfromtimestamp(time_unix).date(), time.min, tzinfo=timezone.utc
        ).timestamp()
    )

# See above comment regarding the decorator.
@disk_cache.memoize()
def get_token_block_price(token, block):
    return float(uniswap.get_token_block_price(token, block=block))

def add_daily_token_prices_to_swaps(swaps):
    token_days = set()
    for swap in swaps:
        from_token = swap['path'][0]
        to_token = swap['path'][-1]
        block_time = swap['block_time']
        day_start_time = get_day_start(block_time)
        token_days.add((from_token, day_start_time))
        token_days.add((to_token, day_start_time))
    token_day_prices_usd = {}
    for token, day in tqdm(token_days, desc='Querying token prices at each day (USD)'):
        try:
            token_day_prices_usd[token, day] = get_token_day_price(token, day)
        except UnrecoverableError:
            print("Couldn't get daily price for token. Skipping.")
            pass

    r = []
    for swap in swaps:
        from_token = swap['path'][0]
        to_token = swap['path'][-1]
        block_time = swap['block_time']
        block_number = swap['block_number']
        day_start_time = get_day_start(block_time)
        if (from_token, day_start_time) in token_day_prices_usd:
            swap['from_token_day_price_usd'] = token_day_prices_usd[from_token, day_start_time]
        else:
            continue
        if (to_token, day_start_time) in token_day_prices_usd:
            swap['to_token_day_price_usd'] = token_day_prices_usd[to_token, day_start_time]
        else:
            continue
        r.append(swap)
    return r

def add_block_token_prices_to_swaps(swaps):
    token_blocks = set()
    for swap in swaps:
        from_token = swap['path'][0]
        to_token = swap['path'][-1]
        token_blocks.add((from_token, swap['block_number']))
        token_blocks.add((to_token, swap['block_number']))
    token_prices_eth = {}
    for token, block in tqdm(token_blocks, desc='Querying token prices at each block (ETH)'):
        try:
            token_prices_eth[token, block] = get_token_block_price(token, block={'number': block})
        except UnrecoverableError:
            print("Couldn't get price for token. Skipping.")
            pass

    r = []
    for swap in swaps:
        from_token = swap['path'][0]
        to_token = swap['path'][-1]
        block_number = swap['block_number']
        if (from_token, block_number) in token_prices_eth:
            swap['from_token_price_eth'] = token_prices_eth[from_token, block_number]
        else:
            continue
        if (to_token, block_number) in token_prices_eth:
            swap['to_token_price_eth'] = token_prices_eth[to_token, block_number]
        else:
            continue
        r.append(swap)
    return r

        
def add_block_token_prices_to_swaps_from_spot_prices(swaps, spot_prices):
    blocks_with_prices = list(spot_prices.keys())
    blocks_with_prices_for_each_swap = get_largest_element_sequence(
        swaps,
        blocks_with_prices,
        lambda swap, block: block <= swap['block_number']
    )
    r = []
    for swap, block_with_prices in tqdm(
        zip(swaps, blocks_with_prices_for_each_swap),
        "Adding spot prices per block"
    ):
        from_token = swap['path'][0]
        to_token = swap['path'][-1]
        if not ({from_token, to_token} <= set(spot_prices[block_with_prices].keys())):
            continue
        swap['from_token_price_eth'] = spot_prices[block_with_prices][from_token]
        swap['to_token_price_eth'] = spot_prices[block_with_prices][to_token]
        r.append(swap)
    return r

def compute_exchange_rate(path, reserves, block):
    xrate = 1
    for t1, t2 in zip(path[:-1], path[1:]):
        if (t1, t2) in reserves.keys():
            reserve1, reserve2 = reserves[t1, t2][block]
        else:
            assert t2, t1 in reserves.keys()
            reserve2, reserve1 = reserves[t2, t1][block]
        xrate *= reserve2 / reserve1
    return xrate

def get_spot_prices_in_eth_from_dune(swaps, pool_ids, token_info, block_interval = 1):
    from_block = min(s['block_number'] for s in swaps)
    to_block = max(s['block_number'] for s in swaps)
    reserves = get_reserves_from_dune(from_block - 1, to_block, pool_ids, token_info)
    reserves = compute_reserves_for_blocks(range(from_block, to_block + 1), reserves)

    token_graph = Graph(list(reserves.keys()))

    for t1, t2 in reserves.keys():
        block = list(reserves[t1, t2].keys())[0]
        token_graph[t1][t2]['weight'] = -reserves[t1, t2][block][0] * reserves[t1, t2][block][1]
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    tokens = {t1 for (t1, _) in reserves.keys()} | {t2 for (_, t2) in reserves.keys()}
    tokens = {t for t in tokens if has_path(token_graph, t, WETH)}
    exchange_paths = {t: shortest_path(token_graph, t, WETH) for t in tokens}
    prices = {}
    for block in tqdm(range(from_block, to_block + 1, block_interval), "Setting spot prices for every block"):
        block_prices = {}
        for t in tokens:
            xrate = compute_exchange_rate(exchange_paths[t], reserves, block)
            if xrate is None:
                continue
            block_prices[t] = xrate
        prices[block] = block_prices
    return prices

def swap_to_order(swap, token_info):
    order = {}
    amm_path = [token_info[token_id]['symbol'] for token_id in swap['path']]
    amm_amounts = [
        a * 10 ** -int(token_info[t]['decimals'])
        for a, t in zip(swap['output_amounts'], swap['path'])
    ]

    order['sellToken'] = amm_path[0]
    order['buyToken'] = amm_path[-1]
    order['sellTokenDailyPriceUSD'] = swap['from_token_day_price_usd']
    order['buyTokenDailyPriceUSD'] = swap['to_token_day_price_usd']
    #order['sellTokenPriceETH'] = swap['from_token_price_eth']
    #order['buyTokenPriceETH'] = swap['to_token_price_eth']

    if swap['sell_amount'] == -1:
        order['maxSellAmount'] = amm_amounts[0]
        min_buy_amount = swap['buy_amount'] * \
            10 ** -int(token_info[swap['path'][-1]]['decimals'])
        order['minBuyAmount'] = min_buy_amount
        if min_buy_amount == 0: # This happens infrequently, don't know what it means
            min_buy_amount = amm_amounts[-1]
        order['isSellOrder'] = True
    else:
        assert swap['buy_amount'] == -1
        order['maxBuyAmount'] = amm_amounts[-1]
        max_sell_amount = swap['sell_amount'] * \
            10 ** -int(token_info[swap['path'][0]]['decimals'])
        if max_sell_amount == 0: # This happens infrequently, don't know what it means
            max_sell_amount = amm_amounts[0]
        order['maxSellAmount'] = max_sell_amount
        order['isSellOrder'] = False

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
    order['address'] = swap['address']
    return order
    
def filter_tokens_with_no_value(swaps, min_usd=1e-2):
    #remove trades between very low price tokens
    return [
        o
        for o in swaps
        if o['from_token_day_price_usd'] >= min_usd and
        o['to_token_day_price_usd'] >= min_usd
    ]

def swap_is_accepted(swap, accepted_tokens):
    return set(swap['path']) <= accepted_tokens

def restrict_to_top_most_traded_tokens_by_nr_trades(swaps, nr_tokens):
    swap_count_per_token = {}
    for s in swaps:
        for t in s['path']:
            if t not in swap_count_per_token.keys():
                swap_count_per_token[t] = 1
            else:
                swap_count_per_token[t] += 1

    tokens = sorted(list(swap_count_per_token.keys()), key=lambda t: swap_count_per_token[t], reverse=True)
    tokens = set(tokens[:nr_tokens])
    return [s for s in swaps if swap_is_accepted(s, tokens)] 

def process(csv_filename, max_nr_tokens, output_filename):
    #token_info = load_tokens(tokens_filename)
    swaps = load_swaps(csv_filename)
    swaps = remove_duplicate_swaps_in_same_block_index(swaps)
    #swaps = filter_swaps(swaps, token_info.keys())
    if max_nr_tokens is not None:
        swaps = restrict_to_top_most_traded_tokens_by_nr_trades(swaps, max_nr_tokens)
    token_info = get_token_infos(swaps)  # this gets info also for intermediate tokens
    #pool_ids = get_all_pool_ids()
    pool_ids = frozendict(get_pools_ids(swaps))
    swaps = add_amm_balances_to_swaps(swaps, pool_ids) #, token_info)
    swaps = add_daily_token_prices_to_swaps(swaps)
    #swaps = filter_tokens_with_no_value(swaps)
    
    
    #spot_prices = get_spot_prices_in_eth_from_dune(swaps, pool_ids, token_info)
    #swaps = add_block_token_prices_to_swaps_from_spot_prices(swaps, spot_prices)
    orders = [swap_to_order(swap, token_info) for swap in swaps]

    #first_block = list(spot_prices.keys())[0]
    #last_block = list(spot_prices.keys())[-1]
    #spot_prices_every_15m = {
    #    b: {token_info[t]['symbol']: v for t, v in spot_prices[b].items()} 
    #    for b in range(first_block, last_block + 1, 60)
    #}

    with open(output_filename, 'w+') as f:
        json.dump({
            'orders': orders,
            #'spot_prices': spot_prices_every_15m
        }, f, indent=2)


parser = argparse.ArgumentParser(
    description='Creater an OBA raw instance file from blockchain data via the csv file generated by '
    'https://explore.duneanalytics.com/queries/9536/source?p_from_block=11093000&p_to_block=11093010#18935'
)
parser.add_argument(
    'filename',
    type=str,
    help='Path to input csv file.'
)

parser.add_argument(
    '--max_nr_tokens',
    type=int,
    default=None,
    help='Only use swaps from the top N traded tokens (by number of swaps)'
)

parser.add_argument(
    'oba_file',
    type=str,
    help='Name of output OBA raw instance json.'
)

args = parser.parse_args()

process(args.filename, args.max_nr_tokens, args.oba_file)
