from .download_swaps import get_swaps
from .utils import is_there_a_opposite_match_in_next_k_blocks

use_cache = True
waiting_time = 10
threshold_for_showing = 0.5

print("Probability of match after waiting", waiting_time, "blocks")

swaps_by_block = get_swaps(use_cache)
sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)
focus_pairs = [
    [o['sellToken'], o['buyToken']]
    for j in range(1, len(sorted_blocks) - 1)
    for o in swaps_by_block.get(sorted_blocks[j], [])
]
focus_pairs = list({(tuple(t)) for t in focus_pairs})
focus_pairs.append(['WETH', 'USDT', 'DAI', 'USDC'])
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

pairs_meeting_threshold = 0
for (key, value) in results.items():
    if value > threshold_for_showing:
        print(key)
        print(value)
        pairs_meeting_threshold += 1

print(pairs_meeting_threshold / len(focus_pairs),
      " pairs of all pairs meet the threshold of a",
      threshold_for_showing, " chance to find a match")
