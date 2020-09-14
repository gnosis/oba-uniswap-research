from networkx.algorithms.cuts import volume
from .subgraph import UniswapClient
import pickle

# Donwloading data from TheGraph takes a bit. When developing/debugging, enabling this
# caches the results on disk.
use_cache=True
focus_pair={'DAI', 'WETH'}
#focus_pair={'WETH', 'WBTC'}
#focus_pair={'SUSHI', 'USDT'}

def get_uniswap_swaps():
    uniswap = UniswapClient()

    # Download data from start_block to end_block.
    end_block = 10826985
    start_block = end_block - (2*24*60*60 // 17)
    swap_transactions = uniswap.get_swaps({'block_number_gte': start_block, 'block_number_lte': end_block})

    swaps_by_block = dict()

    for swap_transaction in swap_transactions:
        for swap in swap_transaction.swaps: 
            if float(swap.amount0_in) > 0:
                sell_token = swap.pair.token0.symbol
                buy_token = swap.pair.token1.symbol
                sell_amount = swap.amount0_in
                buy_amount = swap.amount1_out
                volume = swap.amount_usd
            else:
                sell_token = swap.pair.token1.symbol
                buy_token = swap.pair.token0.symbol
                sell_amount = swap.amount1_in
                buy_amount = swap.amount0_out
                volume = swap.amount_usd
            o = {
                'sellToken': sell_token,
                'buyToken': buy_token,
                'sellAmount': sell_amount,
                'buyAmount': buy_amount,
                'volume': volume
            }
            block_number = int(swap_transaction.block_number)
            if block_number not in swaps_by_block.keys():
                swaps_by_block[block_number] = []
            swaps_by_block[block_number].append(o)
    return swaps_by_block

if not use_cache:
    swaps_by_block = get_uniswap_swaps()
    with open("uniswap_swaps.pickled", "bw+") as f:
        pickle.dump(swaps_by_block, f)
else:
    with open("uniswap_swaps.pickled", "br") as f:
        swaps_by_block = pickle.load(f)

#print(swaps_by_block)
#print({frozenset([o['sellToken'], o['buyToken']]) for b in swaps_by_block.values() for o in b})
prob_opposite_offer = dict()
prob_match = dict()
expected_volume = dict()
expected_nr_trades = dict()

for k in range(10):
    sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)
    nr_blocks_with_at_least_one_direct_trade = 0
    nr_blocks_with_at_least_one_opposite_offer =0
    avg_volume = 0
    nr_direct_trades = 0
    for i in range(len(sorted_blocks) - k):
        block_i = sorted_blocks[i]
        sell_tokens_i = {o['sellToken'] for o in swaps_by_block.get(block_i, [])}
        buy_tokens_i = {o['buyToken'] for o in swaps_by_block.get(block_i, [])}
        sell_tokens_j = {
            o['sellToken']
            for j in range(block_i, block_i + k + 1)
            for o in swaps_by_block.get(j, [])
        }
        buy_tokens_j = {
            o['buyToken']
            for j in range(block_i, block_i + k + 1)
            for o in swaps_by_block.get(j, [])
        }        

        if focus_pair is not None:
            t_1 = tuple(focus_pair)
            t_2 = tuple(reversed(t_1))
            found = False
            singleTradeFound = False
            if t_1[0] in buy_tokens_j and \
                  t_1[1] in sell_tokens_j:
                    nr_blocks_with_at_least_one_opposite_offer += 1
            for t in [t_1, t_2]:
                if t[0] in sell_tokens_i and t[0] in buy_tokens_j and \
                  t[1] in buy_tokens_i and t[1] in sell_tokens_j:
                    sell_tokens_i = buy_tokens_j = {t[0]}
                    buy_tokens_i = sell_tokens_j = {t[1]}
                    found = True
                    break
            if not found:
                sell_tokens_i = sell_tokens_j = buy_tokens_i = buy_tokens_j = set()

        common_tokens = (sell_tokens_i & buy_tokens_j) | (sell_tokens_j & buy_tokens_i)
        if len(common_tokens)>0:
            nr_blocks_with_at_least_one_direct_trade += 1
            for t in common_tokens:
                vols_selling_t_i = [
                    float(o['volume'])
                    for o in swaps_by_block.get(block_i, [])
                    if o['sellToken'] == t
                ]
                vols_selling_t_j = [
                    float(o['volume'])
                    for j in range(block_i, block_i + k + 1)
                    for o in swaps_by_block.get(j, [])
                ]
                avg_volume += sum(vols_selling_t_i) + sum(vols_selling_t_j)
                nr_direct_trades += len(vols_selling_t_i) + len(vols_selling_t_j)
    avg_volume /= nr_direct_trades

    prob_opposite_offer[k] = nr_blocks_with_at_least_one_opposite_offer / (len(sorted_blocks) - k)
    prob_match[k] = nr_blocks_with_at_least_one_direct_trade / (len(sorted_blocks) - k)
    expected_volume[k] = avg_volume
    expected_nr_trades[k] = nr_direct_trades / nr_blocks_with_at_least_one_direct_trade

print("Probability of opposite offer")
for k, v in prob_opposite_offer.items():
    print(k, v)

print("Probability of trade")
for k, v in prob_match.items():
    print(k, v)

print("Expected volume | trade exists")
for k, v in expected_volume.items():
    print(k, v)

print("Expected nr trades | trade exists")
for k, v in expected_nr_trades.items():
    print(k, v)
