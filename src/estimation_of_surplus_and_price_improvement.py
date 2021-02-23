"""
The following program calculates the amount of weth that can be matched in GP
protocol under certain assumptions.
The code separates the uniswap orders into batches and then checks for each batch
how much volume is traded in WETH->Stablecoin and Stablecoin->WETH.
The matched volume will then be min(vol(WETH->Stablecoin), vol(Stablecoin->WETH)).
Stablecoins can be USDC, DAI, USDT.
"""
import matplotlib.pyplot as plt
from .download_swaps import get_swaps
from .utils import find_order_in_next_k_blocks, generate_focus_pairs, filter_out_arbitrageur_swaps, plot_match_survivor
from .read_csv import read_swaps_from_csv

# Parameters
use_dune_data = True
consider_swaps_as_splitted_swaps = True
use_cache = True
waiting_time = 4
threshold_for_showing_probability = 0.1
eth_price = 1573

# focus pair is a trade in direction WETH->USDC or WETH->DAI or WETH->USDT
focus_pairs = [('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',  # WETH
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
                '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
                '0xdac17f958d2ee523a2206206994597c13d831ec7')]  # USDT

print("Probability of match after waiting", waiting_time, "blocks")


migration_percentages = [5, 10, 15, 20, 25, 30, 50, 99]

for migration_percentage in migration_percentages:
    print("Probability with migration precentage of ", migration_percentage)

    # Loads the data according to the set parameters
    if use_dune_data:
        swaps_by_block = read_swaps_from_csv(
            'data/dune_download/swaps_data_from_router_11790000-11791000.csv', consider_swaps_as_splitted_swaps, migration_percentage)
    else:
        swaps_by_block = get_swaps(use_cache, "data/uniswap_swaps.pickled")

    for block in range(min(swaps_by_block.keys()), max(swaps_by_block.keys())):
        if block not in swaps_by_block.keys():
            swaps_by_block[block] = []

    # filtering out arbitrageurs
    swaps_by_block = filter_out_arbitrageur_swaps(swaps_by_block)
    # sorts blocks
    sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)
    counts = []
    # For each focus pair, it calculate the probability
    for focus_pair in focus_pairs:
        surplus_usd = 0  # surplus measued in usd
        non_matchable_volume = 0  # Volume measued in usd
        matched_volume = 0  # Volume measued in usd
        for batch_interval in range((len(sorted_blocks))//waiting_time):
            amount_stable_weth = 0  # Volume measued in weth
            amount_weth_stable = 0  # Volume measured in weth
            cnt = 0
            for block_index in range(batch_interval * waiting_time, (batch_interval+1) * waiting_time):
                for o in swaps_by_block.get(sorted_blocks[block_index], []):
                    if focus_pair[0] == o['buyToken'] and \
                            (focus_pair[1] == o['sellToken'] or
                             (len(focus_pair) >= 3 and focus_pair[2] == o['sellToken']) or
                             (len(focus_pair) >= 4 and focus_pair[3] == o['sellToken'])):
                        # getting the amount of weth bought
                        cnt += 1
                        amount_stable_weth += o['buyAmount'] // 10**18

                    if focus_pair[0] == o['sellToken'] and \
                            (focus_pair[1] == o['buyToken'] or
                             (len(focus_pair) >= 3 and focus_pair[2] == o['buyToken']) or
                             (len(focus_pair) >= 4 and focus_pair[3] == o['buyToken'])):
                        # getting the amount of weth sold
                        cnt += 1
                        amount_weth_stable += o['sellAmount'] // 10**18

            surplus_usd_per_batch = min(
                amount_stable_weth, amount_weth_stable) * 2 * 0.003 * eth_price
            non_matchable_volume_usd_per_batch = abs(
                amount_stable_weth-amount_weth_stable) * eth_price
            counts.append(non_matchable_volume_usd_per_batch)
            surplus_usd += surplus_usd_per_batch
            matched_volume += min(
                amount_stable_weth, amount_weth_stable) * 2 * eth_price
            non_matchable_volume += non_matchable_volume_usd_per_batch
    print("Matchable volume in", len(sorted_blocks), " blocks:",
          matched_volume, "[USD]")
    print("Non-matchable volume in ", len(sorted_blocks), " blocks:",
          non_matchable_volume, "[USD]")
    if (surplus_usd+non_matchable_volume > 0):
        print("Percentable of matchable volume in ", len(sorted_blocks), " blocks", matched_volume /
              (matched_volume+non_matchable_volume))
    investigated_time_period_in_sec = len(sorted_blocks) * 15
    print("Total surplus in USD for 30 days would be",
          surplus_usd * 24 * 60 * 60 * 30 / investigated_time_period_in_sec, " plus saved slippage")
    print("That would imply an on-average price improvement of",
          surplus_usd / (matched_volume+non_matchable_volume) * 100, "[%]")
# print(counts)
# counts = [float(c) for c in counts if c > 0]
# plt.hist(counts, bins=100)
# plt.show()
# print(sum(counts)/len(counts))
