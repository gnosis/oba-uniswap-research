from .download_swaps import get_swaps
from .utils import is_there_a_opposite_match_in_next_k_blocks

# Parameters
use_cache = True
number_of_blocks_looking_forward = 50
focus_pairs = [['WETH', 'USDT', 'DAI', 'USDC'], ['WETH', 'DAI'],
               ['WETH', 'USDT'], ['WETH', 'USDC'], ['WETH', 'MKR'],
               ['WETH', 'GNO'], ['WETH', 'SUSHI'], ['WETH', 'SWRV'],
               ['WETH', 'WBTC'], ['WETH', 'YFI'], ['WETH', 'LINK']]


print("Expected waiting time for opposite offer in block:")
# Description
# The following program calculates the expected waiting time for finding a
# match - a counter order - for a random order
# The calculation makes the assumption that the appearance of a counter order
# is independent of placing the random order.

swaps_by_block = get_swaps(use_cache)

sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)
for focus_pair in focus_pairs:
    prob_opposite_offer = []
    for k in range(number_of_blocks_looking_forward):
        nr_blocks_with_at_least_one_opposite_offer = 0
        for i in range(len(sorted_blocks) - k):
            if is_there_a_opposite_match_in_next_k_blocks(i,
                                                          k,
                                                          focus_pair,
                                                          swaps_by_block,
                                                          sorted_blocks):
                nr_blocks_with_at_least_one_opposite_offer += 1

        prob_opposite_offer.append(nr_blocks_with_at_least_one_opposite_offer /
                                   (len(sorted_blocks) - k))

    expected_waiting_time_for_user = 0
    last_probability = 0
    for k, v in enumerate(prob_opposite_offer, 1):
        expected_waiting_time_for_user += k*(v-last_probability)
        last_probability = v
    print(focus_pair)
    if last_probability > 0.98:
        print(expected_waiting_time_for_user)
    else:
        print("Increase number_of_blocks_looking_forward to get"
              "a reasonable calculation")
