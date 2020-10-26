from .download_swaps import get_swaps
from .utils import is_there_a_opposite_match_in_next_k_blocks
from .read_csv import read_swaps_from_csv

# Parameters
use_dune_data = True
consider_swaps_as_splitted_swaps = True
use_cache = True
waiting_time = 10
threshold_for_showing_probability = 0.5

print("Probability of match after waiting", waiting_time, "blocks")
# Description
# The following program calculates the probability of finding a match - a
# counter order - for a random order.
# The calculation makes the assumption that the appearance of a counter order
# is independent of placing the random order.


# Loads the data according to the set parameters
if use_dune_data:
    swaps_by_block = read_swaps_from_csv(
        'data/swaps_data_from_router.csv', consider_swaps_as_splitted_swaps)
else:
    swaps_by_block = get_swaps(use_cache, "data/uniswap_swaps.pickled")

# sorts blocks
sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)

# generates all possible pairs
focus_pairs = [
    [o['sellToken'], o['buyToken']]
    for j in range(1, len(sorted_blocks) - 1)
    for o in swaps_by_block.get(sorted_blocks[j], [])
]
focus_pairs = list({(tuple(t)) for t in focus_pairs})

# For each focus pair, it calculate the probability
results = dict()
for focus_pair in focus_pairs:
    nr_of_times_a_match_can_be_found = 0
    for i in range(len(sorted_blocks) - waiting_time):
        if is_there_a_opposite_match_in_next_k_blocks(i,
                                                      waiting_time,
                                                      focus_pair,
                                                      swaps_by_block,
                                                      sorted_blocks):
            nr_of_times_a_match_can_be_found += 1

    prob_opposite_offer = nr_of_times_a_match_can_be_found / \
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
