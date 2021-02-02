"""
The following program calculates the probability of finding a match - a
counter order - for a Token->StableCoin order. The coincidence of wants will be
found on WETH-USDC or WETH-DAI or WETH-USDT pair after the Token->StableCoin order is splitted into Token->WETH and WETH->StableCoin. 

Formally, it computes,
p(counter_order_in_next_k_blocks | order_in_this_block)

The calculation makes the assumption that the appearance of an order and
a counter order is independent.

That is,
p(counter_order_in_next_k_blocks, order_in_this_block) = \
    p(counter_order_in_next_k_blocks) * p(order_in_this_block)

Under this assumption,

p(counter_order_in_next_k_blocks | order_in_this_block) = \
    = p(counter_order_in_next_k_blocks, order_in_this_block) / \
        p(order_in_this_block)
    = p(counter_order_in_next_k_blocks) = p(order_in_next_k_blocks)

This experimental setup assumes intelligent batching of the driver: 
A batch does  not have a given starting time, rather it starts with
the posting of a new order. Orders are only settled, if either, the
driver finds already a coincidence of wants or the order is about to
expire (assuming a validity of waiting_time=x blocks).
"""

from .download_swaps import get_swaps
from .utils import find_order_in_next_k_blocks, generate_focus_pairs, filter_out_arbitrageur_swaps, plot_match_survivor
from .read_csv import read_swaps_from_csv

# Parameters
use_dune_data = True
consider_swaps_as_splitted_swaps = True
use_cache = True
waiting_time = 4
threshold_for_showing_probability = 0.1

# focus pair is a trade in direction WETH->USDC or WETH->DAI or WETH->USDT
focus_pairs = [('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',  # WETH
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
                '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
                '0xdac17f958d2ee523a2206206994597c13d831ec7')]  # USDT

print("Probability of match after waiting", waiting_time, "blocks")


migration_percentages = [5, 10, 15, 20, 25, 30, 50]

for migration_percentage in migration_percentages:
    print("Probability with migration precentage of ", migration_percentage)

    # Loads the data according to the set parameters
    if use_dune_data:
        swaps_by_block = read_swaps_from_csv(
            'data/dune_download/swaps_data_from_router_11740000-11741000.csv', consider_swaps_as_splitted_swaps, migration_percentage)
    else:
        swaps_by_block = get_swaps(use_cache, "data/uniswap_swaps.pickled")

    for block in range(min(swaps_by_block.keys()), max(swaps_by_block.keys())):
        if block not in swaps_by_block.keys():
            swaps_by_block[block] = []

    # filtering out arbitrageurs
    swaps_by_block = filter_out_arbitrageur_swaps(swaps_by_block)

    # sorts blocks
    sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)

    # For each focus pair, it calculate the probability
    results = dict()
    for focus_pair in focus_pairs:
        nr_of_times_an_order_can_be_found = 0
        for block_index in range(len(sorted_blocks) - waiting_time):
            if find_order_in_next_k_blocks(
                block_index,
                waiting_time,
                focus_pair,
                swaps_by_block,
                sorted_blocks
            ):
                nr_of_times_an_order_can_be_found += 1

        prob_opposite_offer = nr_of_times_an_order_can_be_found / \
            (len(sorted_blocks) - waiting_time)
        results["-".join(focus_pair)] = prob_opposite_offer

    # prints the pairs meeting the threshold: threshold_for_showing_probability
    pairs_meeting_threshold = 0
    for (key, value) in results.items():
        print(key)
        print(value)
        pairs_meeting_threshold += 1
