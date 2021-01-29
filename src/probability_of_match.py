"""
The following program calculates the probability of finding a match - a
counter order - for a random order. 

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
"""

from .download_swaps import get_swaps
from .utils import find_order_in_next_k_blocks, generate_focus_pairs, filter_out_arbitrageur_swaps, plot_match_survivor
from .read_csv import read_swaps_from_csv

# Parameters
use_dune_data = True
consider_swaps_as_splitted_swaps = True
use_cache = True
waiting_time = 4
threshold_for_showing_probability = 0.5
percentage_of_migration_from_uniswap = 50

print("Probability of match after waiting", waiting_time, "blocks")

# Loads the data according to the set parameters
if use_dune_data:
    swaps_by_block = read_swaps_from_csv(
        'data/dune_download/merged.csv', consider_swaps_as_splitted_swaps, percentage_of_migration_from_uniswap)
else:
    swaps_by_block = get_swaps(use_cache, "data/uniswap_swaps.pickled")

for block in range(min(swaps_by_block.keys()), max(swaps_by_block.keys())):
    if block not in swaps_by_block.keys():
        swaps_by_block[block] = []

# filtering out arbitrageurs
swaps_by_block = filter_out_arbitrageur_swaps(swaps_by_block)

# sorts blocks
sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)

# generates all possible pairs
focus_pairs = generate_focus_pairs(sorted_blocks, swaps_by_block)

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
    if value > threshold_for_showing_probability:
        print(key)
        print(value)
        pairs_meeting_threshold += 1

print(pairs_meeting_threshold / len(focus_pairs),
      " pairs of all pairs meet the threshold of a",
      threshold_for_showing_probability, " chance to find a match")


# prints the number of pairs with likelihoods above certain thresholds

print("An overview of the number of pairs matchable with different thresholds")
thresholds = [0.2, 0.3, 0.4, 0.5, 0.6]

for threshold in thresholds:
    pairs_meeting_threshold = 0
    for (key, value) in results.items():
        if value > threshold:
            pairs_meeting_threshold += 1
    print(threshold, ":", pairs_meeting_threshold)

plot_match_survivor(results)
