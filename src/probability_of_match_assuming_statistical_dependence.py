from .download_swaps import get_swaps


# Donwloading data from TheGraph takes a bit. When developing/debugging, enabling this
# caches the results on disk.
use_cache = True
focus_pair = {'WETH', 'DAI', 'USDC', 'USDT'}
#focus_pair={'WETH', 'DAI', 'DAI' , 'DAI'}
#focus_pair={'WETH', 'WBTC'}
#focus_pair={'SUSHI', 'WETH'}


swaps_by_block = get_swaps(use_cache)

# print(swaps_by_block)
#print({frozenset([o['sellToken'], o['buyToken']]) for b in swaps_by_block.values() for o in b})
prob_opposite_offer = dict()
prob_match = dict()
expected_volume = dict()
expected_nr_trades = dict()

for k in range(20):
    sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)
    nr_blocks_with_at_least_one_direct_trade = 0
    nr_blocks_with_at_least_one_opposite_offer = 0
    avg_volume = 0
    nr_direct_trades = 0
    for i in range(len(sorted_blocks) - k):
        block_i = sorted_blocks[i]
        sell_tokens_i = {o['sellToken']
                         for o in swaps_by_block.get(block_i, [])}
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
                (t_1[1] in sell_tokens_j or
                 (len(t_1) >= 3 and t_1[2] in sell_tokens_j) or
                 (len(t_1) >= 4 and t_1[3] in sell_tokens_j)
                 ):
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

        common_tokens = (sell_tokens_i & buy_tokens_j) | (
            sell_tokens_j & buy_tokens_i)
        if len(common_tokens) > 0:
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
                nr_direct_trades += len(vols_selling_t_i) + \
                    len(vols_selling_t_j)
    if nr_direct_trades > 0:
        avg_volume /= nr_direct_trades
    else:
        avg_volume = 0
    prob_opposite_offer[k] = nr_blocks_with_at_least_one_opposite_offer / \
        (len(sorted_blocks) - k)
    prob_match[k] = nr_blocks_with_at_least_one_direct_trade / \
        (len(sorted_blocks) - k)
    expected_volume[k] = avg_volume
    expected_nr_trades[k] = nr_direct_trades / \
        nr_blocks_with_at_least_one_direct_trade

print("Probability of opposite offer")
for k, v in prob_opposite_offer.items():
    print(v)

print("Probability of trade")
for k, v in prob_match.items():
    print(k, v)

print("Expected volume | trade exists")
for k, v in expected_volume.items():
    print(k, v)

print("Expected nr trades | trade exists")
for k, v in expected_nr_trades.items():
    print(k, v)
