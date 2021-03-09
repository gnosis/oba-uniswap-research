import json
import argparse
from math import ceil

parser = argparse.ArgumentParser(
    description='Convert an OBA file to a gp v2 (problem) instance file.')

parser.add_argument(
    'oba_file',
    type=str,
    help='Path to input OBA instance file.'
)

parser.add_argument(
    'gpv2_file',
    type=str,
    help='Path to output gp v2 instance file.'
)

parser.add_argument(
    '--exclude_market_makers',
    type=bool,
    default=False,
    help='Exclude market makers.'
)

parser.add_argument(
    '--default_fee',
    type=float,
    default=0,
    help='Default gp fee.'
)

args = parser.parse_args()

with open(args.oba_file, 'r') as f:
    d = json.load(f)

exclude_market_makers = args.exclude_market_makers
default_fee = args.default_fee

tokens = {}
for t in d['tokens']:
    tokens[t] = {
        'decimals': 18,  # We will use 18 decimal digits for all tokens
        'normalize_priority': 1 if t in ['DAI','USDC','USDT','OWL'] else 0
    }

orders = {}
for oid, o in d['orders'].items():
    if exclude_market_makers and not o['fillOrKill']:
        continue
    sell_token = o['sellToken']
    buy_token = o['buyToken']
    xrate = { t: p for p, t in o['limitXRate']}
    if o['maxSellAmount'] is not None:
        sell_amount = int(o['maxSellAmount'] * 10**18)
        buy_amount = int(ceil(sell_amount * xrate[buy_token] / xrate[sell_token]))
    else:
        assert o['maxBuyAmount'] is not None
        buy_amount = int(o['maxBuyAmount'] * 10**18)
        sell_amount = int(ceil(buy_amount * xrate[sell_token] / xrate[buy_token]))
    
    orders[oid] = {
        'sell_token': sell_token,
        'buy_token': buy_token,
        'sell_amount': str(sell_amount),
        'buy_amount': str(buy_amount),
        'is_sell_order': o['maxSellAmount'] is not None,
        'allow_partial_fill': not o['fillOrKill']
    }

uniswaps = {}
for uid, u in d['uniswaps'].items():
    uniswaps[uid] = {
        'token1': u['token1'],
        'token2': u['token2'],
        'balance1': str(int(round(u['balance1'] * 10**18))),
        'balance2': str(int(round(u['balance2'] * 10**18))),
        'mandatory': True,
        'fee': "0.003"
    }

batch_auction = {
    'tokens': tokens,
    'orders': orders,
    'uniswaps': uniswaps,
    'default_fee': default_fee
}

with open(args.gpv2_file, 'w+') as f:
    json.dump(batch_auction, f, indent=4)
