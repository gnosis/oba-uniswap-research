"""
The following program calculates the probability of finding a match - a
counter order - for a random order. 

Formally, it computes,
p(counter_order_in_next_k_blocks | order_in_this_block)

The calculation makes the assumption that the appearance of an order and
a counter order is NOT independent.

That is,
p(counter_order_in_next_k_blocks, order_in_this_block) != \
    p(counter_order_in_next_k_blocks) * p(order_in_this_block)
"""

from .download_swaps import get_swaps
from .utils import find_order_in_block, find_order_in_next_k_blocks, filter_out_arbitrageur_swaps, plot_match_survivor, generate_focus_pairs
from .read_csv import read_swaps_from_csv

# Parameters
use_dune_data = True
consider_swaps_as_splitted_swaps = True
use_cache = True
waiting_time = 4
threshold_for_showing_probability = 0.5

print("Probability of match after waiting", waiting_time, "blocks")

# Loads the data according to the set parameters
if use_dune_data:
    swaps_by_block = read_swaps_from_csv(
        'data/dune_download/merged.csv', consider_swaps_as_splitted_swaps)
else:
    swaps_by_block = get_swaps(use_cache, "data/uniswap_swaps.pickled")


# filtering out arbitrageurs
swaps_by_block = filter_out_arbitrageur_swaps(swaps_by_block)

# sorts blocks
sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)

# Sets a threshold for the minimum amount of appearances of a swap pair
# Assuming a swap happens on average less frequently than in each 20ths block
# the pair should not be relevant for our exchange
threshold_for_min_nr_of_appearances_to_be_considered = len(sorted_blocks)/20

# generates all possible pairs
focus_pairs = generate_focus_pairs(sorted_blocks, swaps_by_block)


# For each focus pair, it calculate the probability
results = dict()
nr_of_orders_in_total = 0
nr_of_orders_matchable = 0
for focus_pair in focus_pairs:
    nr_of_times_an_order_can_be_found = 0
    nr_of_times_a_counter_order_can_be_found_if_order_is_found = 0
    for start_block_index in range(len(sorted_blocks) - waiting_time):
        if not find_order_in_block(
            sorted_blocks[start_block_index], focus_pair, swaps_by_block
        ):
            continue
        nr_of_times_an_order_can_be_found += 1
        nr_of_orders_in_total += 1
        found = find_order_in_next_k_blocks(
            start_block_index,
            waiting_time,
            tuple(reversed(focus_pair)),
            swaps_by_block,
            sorted_blocks
        )
        if found:
            nr_of_times_a_counter_order_can_be_found_if_order_is_found += 1
            nr_of_orders_matchable += 1
    prob_opposite_offer = nr_of_times_a_counter_order_can_be_found_if_order_is_found / \
        nr_of_times_an_order_can_be_found \
        if nr_of_times_an_order_can_be_found > \
        threshold_for_min_nr_of_appearances_to_be_considered else 0
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

print("From ", nr_of_orders_in_total, "checked orders, for ",
      nr_of_orders_matchable, " the algorithm was able to find a match")
# plot_match_survivor(results)
