import argparse
import json
import random
from math import ceil
from pathlib import Path

import networkx as nx
from networkx.algorithms.components import connected


def timestamp(o):
    return o['timestamp'] if 'timestamp' in o else o['uniswap']['timestamp']

# Create a pool for every uniswap pool used in any order.
def extract_uniswap_pools(orders):
    pools = {}
    for o in orders:
        if 'uniswap' not in o.keys():
            continue
        for i in range(len(o['uniswap']['path']) - 1):
            token1 = o['uniswap']['path'][i]
            token2 = o['uniswap']['path'][i + 1]
            # do not add pool if pool was already added
            # (chronologically first should be used)
            if (token1, token2) in pools.keys() or (token2, token1) in pools.keys():
                continue
            balance1 = o['uniswap']['balancesSellToken'][i]
            balance2 = o['uniswap']['balancesBuyToken'][i]
            pools[token1, token2] = {
                'token1': token1,
                'token2': token2,
                'balance1': balance1,
                'balance2': balance2,
                'mandatory': False
            }

    return {'-'.join(k): v for k, v in pools.items()}

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

def create_batch(orders, limit_xrate_relax_frac):
    pools = extract_uniswap_pools(orders)
    orders = filter_unmatchable_orders(orders, pools)
    nr_mm_orders=0
    def create_order_id(o):
        if o['fillOrKill']:
            return f"{o['uniswap']['block']}-{o['uniswap']['index']}"
        else:
            return str(nr_mm_orders)
    def create_order(o):
        r =  {
            'sellToken': o['sellToken'],
            'buyToken': o['buyToken'],
            'isSellOrder': o['isSellOrder'],
            'fillOrKill': o['fillOrKill'],
            'address': o['address']
        }
        if o['isSellOrder']:
            r['maxSellAmount'] = o['maxSellAmount']
            if o['minBuyAmount'] > 0:
                r['minBuyAmount'] = o['minBuyAmount']
            else:
                r['minBuyAmount'] = o['uniswap']['amounts'][-1]
            if limit_xrate_relax_frac >= 0:
                r['minBuyAmount'] /= (1 + limit_xrate_relax_frac)
            else:
                r['minBuyAmount'] = o['uniswap']['amounts'][-1]
            assert r['minBuyAmount'] > 0
            assert r['minBuyAmount'] <= o['uniswap']['amounts'][-1]
        else:
            r['maxBuyAmount'] = o['maxBuyAmount']
            if o['maxSellAmount'] > 0:
                r['maxSellAmount'] = o['maxSellAmount']
            else:
                r['maxSellAmount'] = o['uniswap']['amounts'][0]
            if limit_xrate_relax_frac >= 0:
                r['maxSellAmount'] *= (1 + limit_xrate_relax_frac)
            else:
                r['maxSellAmount'] = o['uniswap']['amounts'][0]
            assert r['maxBuyAmount'] > 0
            assert r['maxSellAmount'] >= o['uniswap']['amounts'][0]

        r['execSellAmount'] = o['uniswap']['amounts'][0]
        r['execBuyAmount'] = o['uniswap']['amounts'][-1]        

        assert r['maxSellAmount'] > 0        
        return r

    orders = {
        create_order_id(o): create_order(o)
        for o in orders
    }

    tokens = {o['sellToken'] for o in orders.values()} | \
        {o['buyToken'] for o in orders.values()} | \
        {p['token1'] for p in pools.values()} | \
        {p['token2'] for p in pools.values()}
    tokens = list(tokens)
    ref_token = 'WETH' if 'WETH' in tokens else \
        (tokens[0] if len(tokens) > 0 else None)

    return {
        'refToken': ref_token,
        'obaFee': 0.001,
        'uniswapFee': 0.003,
        'tokens': tokens,
        'orders': orders,
        'uniswaps': pools
    }

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


def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)

def order_is_accepted(order, accepted_tokens):
    if 'uniswap' in order.keys():
        return set(order['uniswap']['path']) <= accepted_tokens
    else:
        {order['sellToken'], order['buyToken']} <= accepted_tokens

def restrict_to_top_most_traded_tokens_by_nr_trades(oba_orders, nr_tokens):
    swap_count_per_token = {}
    for o in oba_orders:
        for t in [o['sellToken'], o['buyToken']]:
            if t not in swap_count_per_token.keys():
                swap_count_per_token[t] = 1
            else:
                swap_count_per_token[t] += 1

    tokens = sorted(list(swap_count_per_token.keys()), key=lambda t: swap_count_per_token[t], reverse=True)
    tokens = set(tokens[:nr_tokens])
    return [o for o in oba_orders if order_is_accepted(o, tokens)] 


def get_top_most_traded_tokens_by_vol(oba_orders, nr_tokens):
    swap_vol_per_token = {}
    for o in oba_orders:
        if o['isSellOrder']:
            traded_vol = o['maxSellAmount'] * o['sellTokenPriceETH']
        else:
            traded_vol = o['maxBuyAmount'] * o['buyTokenPriceETH']
        for t in [o['sellToken'], o['buyToken']]:
            if t not in swap_vol_per_token.keys():
                swap_vol_per_token[t] = traded_vol
            else:
                swap_vol_per_token[t] += traded_vol

    tokens = sorted(list(swap_vol_per_token.keys()), key=lambda t: swap_vol_per_token[t], reverse=True)
    tokens = set(tokens[:nr_tokens])
    return tokens

def restrict_to_top_most_traded_tokens_by_vol(oba_orders, nr_tokens):
    tokens = get_top_most_traded_tokens_by_vol(oba_orders, nr_tokens)
    return [o for o in oba_orders if order_is_accepted(o, tokens)] 

def get_users_sorted_by_incr_nr_swaps(oba_orders):
    swap_count_per_user = {}
    for o in oba_orders:
        if 'address' not in o.keys():  # mm orders
            continue
        if o['address'] not in swap_count_per_user.keys():
            swap_count_per_user[o['address']] = 1
        else:
            swap_count_per_user[o['address']] += 1

    users = sorted(list(swap_count_per_user.keys()), key=lambda u: swap_count_per_user[u])
    return users

def restrict_to_user_fraction(oba_orders, keep_fraction):
    users = get_users_sorted_by_incr_nr_swaps(oba_orders)
    users = users[:round(keep_fraction * len(users))]
    return [o for o in oba_orders if o['address'] in users]


def batch_contains_address(batch, addresses):
    return any((o['address'] in addresses) for o in batch['orders'].values())


def split_batch_into_connected_batches(batch):
    g = nx.Graph([(u['token1'], u['token2']) for u in batch['uniswaps'].values()])
    uccs = list(nx.connected_components(g))
    if len(uccs) == 1:
        return [batch]
    batches = []
    for ucc in uccs:
        b = dict(batch)
        b['tokens'] = [
            t
            for t in b['tokens']
            if t in ucc
        ]
        b['orders'] = {
            oid: o
            for oid, o in b['orders'].items()
            if {o['sellToken'], o['buyToken']} <= ucc
        }
        b['uniswaps'] = {
            uid: u
            for uid, u in b['uniswaps'].items()
            if {u['token1'], u['token2']} <= ucc
        }
        batches.append(b)
    return batches


def convert_to_gpv2_instance(batch, default_fee, exclude_market_makers=False):
    tokens = {}
    for t in batch['tokens']:
        tokens[t] = {
            'decimals': 18,  # We will use 18 decimal digits for all tokens
            'normalize_priority': 1 if t in ['DAI','USDC','USDT','OWL'] else 0
        }

    orders = {}
    for oid, o in batch['orders'].items():
        if exclude_market_makers and not o['fillOrKill']:
            continue
        sell_token = o['sellToken']
        buy_token = o['buyToken']        
        if o['isSellOrder']:
            sell_amount = int(o['maxSellAmount'] * 10**18)
            buy_amount = int(ceil(o['minBuyAmount'] * 10**18))
        else:
            buy_amount = int(o['maxBuyAmount'] * 10**18)
            sell_amount = int(o['maxSellAmount'] * 10**18)
        assert sell_amount > 0 and buy_amount > 0
        orders[oid] = {
            'sell_token': sell_token,
            'buy_token': buy_token,
            'sell_amount': str(sell_amount),
            'buy_amount': str(buy_amount),
            'is_sell_order': o['isSellOrder'],
            'allow_partial_fill': not o['fillOrKill'],
            'exec_sell_amount': int(o['execSellAmount'] * 10**18),
            'exec_buy_amount': int(o['execBuyAmount'] * 10**18)
        }

    uniswaps = {}
    for uid, u in batch['uniswaps'].items():
        uniswaps[uid] = {
            'token1': u['token1'],
            'token2': u['token2'],
            'balance1': str(int(round(u['balance1'] * 10**18))),
            'balance2': str(int(round(u['balance2'] * 10**18))),
            'mandatory': True,
            'fee': "0.003"
        }

    return {
        'tokens': tokens,
        'orders': orders,
        'uniswaps': uniswaps,
        'default_fee': default_fee
    }


def main(
    oba_file, output_dir, batch_duration=60, max_nr_instances = None,
    nr_tokens = None, user_fraction=1, default_fee=0, limit_xrate_relax_frac=0.01
):
    oba_orders = load_json(oba_file)['orders']

    # restrict instances to most traded tokens
    if nr_tokens is not None:
        oba_orders = restrict_to_top_most_traded_tokens_by_vol(oba_orders, nr_tokens)

    # restrict instances to less frequent users
    #if user_fraction < 1:
    #    oba_orders = restrict_to_user_fraction(oba_orders, user_fraction)

    # restrict batches to less frequent users
    if user_fraction < 1:
        users_sorted_by_incr_nr_swaps = get_users_sorted_by_incr_nr_swaps(oba_orders)
        more_frequent_users = users_sorted_by_incr_nr_swaps[
            round(user_fraction * len(users_sorted_by_incr_nr_swaps)):
        ]
    else:
        more_frequent_users = None

    batches = []
    for first_idx, last_idx in batch_iterator(oba_orders, batch_duration):
        batch_orders = oba_orders[first_idx:last_idx]
        first_timestamp = timestamp(oba_orders[first_idx])
        last_timestamp = timestamp(oba_orders[last_idx - 1])
        batch = create_batch(batch_orders, limit_xrate_relax_frac)
        if len(batch['orders']) == 0:
            continue
        if more_frequent_users is not None and batch_contains_address(batch, more_frequent_users):
            continue
        batches.append((first_timestamp, last_timestamp, batch))

    # Generate at most max_nr_instances
    if max_nr_instances is not None and len(batches) > max_nr_instances:
        batches = random.sample(batches, max_nr_instances)

    # Create instances
    for first_timestamp, last_timestamp, batch in batches:
        connected_batches = split_batch_into_connected_batches(batch)
        for i, connected_batch in enumerate(connected_batches):
            connected_batch = convert_to_gpv2_instance(connected_batch, default_fee)
            file_suffix = f'-{i+1}' if len(connected_batches) > 1 else ''
            with open(f'{output_dir}/instance_{batch_duration}_{first_timestamp}-{last_timestamp}{file_suffix}.json', 'w+') as f:
                json.dump(connected_batch, f, indent=2)

    # Create restricted perblock file
    oba_orders = [
        o
        for o in oba_orders
        if any(
            o['uniswap']['timestamp'] >= first_timestamp and
            o['uniswap']['timestamp'] <= last_timestamp
            for first_timestamp, last_timestamp, _ in batches
        )]
    with open(Path(output_dir).parent / Path(oba_file).name, "w+") as f:
        json.dump({'orders': oba_orders}, f, indent=2)


parser = argparse.ArgumentParser(
    description='Split a large per_block.json file into several instances, one for each time interval.')


parser.add_argument(
    'per_block_file',
    type=str,
    help='Path to OBA instance file.'
)

parser.add_argument(
    'output_dir',
    type=str,
    help='Path to output directory.'
)

parser.add_argument(
    'batch_duration',
    type=int,
    help='Batch duration in seconds used to "cut" blockchain data into batches.'
)

parser.add_argument(
    '--max_nr_instances',
    type=int,
    default=None,
    help='Generate at most this number of instances, sampling if necessary.'
    ' If not passed generates all.'
)

parser.add_argument(
    '--nr_tokens',
    type=int,
    default=None,
    help='Restrict to the nr_tokens most traded (more trades) tokens. If not passed uses all.'
)

parser.add_argument(
    '--user_fraction',
    type=float,
    default=1,
    help='Restrict to a fraction of (less frequent) users.'
)

parser.add_argument(
    '--limit_xrate_relax_frac',
    type=float,
    default=0.01,
    help='Relax limit exchange rates by this fraction. If < 0 then the effective price is used.'
)

parser.add_argument(
    '--default_fee',
    type=float,
    default=0,
    help='Default gp fee.'
)

args = parser.parse_args()

main(
    args.per_block_file, args.output_dir, args.batch_duration, 
    args.max_nr_instances, args.nr_tokens, args.user_fraction,
    args.default_fee, args.limit_xrate_relax_frac
)
