"""
The following program calculates the amount of weth that can be matched in GP 
protocol under certain assumptions. 
The code separates the uniswap orders into batches and then checks for each batch
how much volume is traded in WETH->Stablecoin and Stablecoin->WETH. 
The matched volume will then be min(vol(WETH->Stablecoin), vol(Stablecoin->WETH)). 
Stablecoins can be USDC, DAI, USDT.
"""

from .download_swaps import get_swaps
from .utils import find_order_in_next_k_blocks, generate_focus_pairs, filter_out_arbitrageur_swaps, plot_match_survivor
from .read_csv import read_swaps_from_csv

# Parameters
use_dune_data = True
consider_swaps_as_splitted_swaps = True
use_cache = True
waiting_time = 8
threshold_for_showing_probability = 0.1
eth_price = 1300

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
            'data/dune_download/swaps_data_from_router_11740000-11741000.csv', consider_swaps_as_splitted_swaps, migration_percentage)
    else:
        swaps_by_block = get_swaps(use_cache, "data/uniswap_swaps.pickled")

    for block in range(min(swaps_by_block.keys()), max(swaps_by_block.keys())):
        if block not in swaps_by_block.keys():
            swaps_by_block[block] = []

    # filtering out arbitrageurs
    # swaps_by_block = filter_out_arbitrageur_swaps(swaps_by_block)
    # sorts blocks
    sorted_blocks = sorted(swaps_by_block.keys(), reverse=True)

    # For each focus pair, it calculate the probability
    for focus_pair in focus_pairs:
        directly_matchable_volume_on_focus_pair = 0  # Volume measued in weth
        non_matchable_volume_on_focus_pair = 0  # Volume measued in weth
        for batch_interval in range((len(sorted_blocks))//waiting_time):
            sum_volume_direction_focus_pair_in_batch = 0  # Volume measued in weth
            sum_volume_direction_invers_focus_pair_in_batch = 0  # Volume measured in weth
            for block_index in range(batch_interval * waiting_time, batch_interval * waiting_time + waiting_time):
                for o in swaps_by_block.get(sorted_blocks[block_index], []):
                    if focus_pair[0] == o['buyToken'] and \
                        (focus_pair[1] == o['sellToken'] or
                            (len(focus_pair) >= 3 and focus_pair[2] == o['sellToken']) or
                         (len(focus_pair) >= 4 and focus_pair[3] == o['sellToken'])):
                        # getting the amount of weth bought
                        sum_volume_direction_focus_pair_in_batch += o['buyAmount']

                    if focus_pair[0] == o['sellToken'] and \
                        (focus_pair[1] == o['buyToken'] or
                            (len(focus_pair) >= 3 and focus_pair[2] == o['buyToken']) or
                         (len(focus_pair) >= 4 and focus_pair[3] == o['buyToken'])):
                        # getting the amount of weth sold
                        sum_volume_direction_invers_focus_pair_in_batch += o['sellAmount']

            directly_matchable_volume_on_focus_pair_in_batch = min(
                sum_volume_direction_focus_pair_in_batch, sum_volume_direction_invers_focus_pair_in_batch)*2
            non_matchable_volume_on_focus_pair_in_batch = abs(
                sum_volume_direction_focus_pair_in_batch-sum_volume_direction_invers_focus_pair_in_batch)
            # print(batch_interval)
            # if(directly_matchable_volume_on_focus_pair_in_batch > 0):
            #     print(directly_matchable_volume_on_focus_pair_in_batch /
            #           (directly_matchable_volume_on_focus_pair_in_batch+non_matchable_volume_on_focus_pair_in_batch))
            directly_matchable_volume_on_focus_pair += directly_matchable_volume_on_focus_pair_in_batch
            non_matchable_volume_on_focus_pair += non_matchable_volume_on_focus_pair_in_batch

    # print("Matchable weth in", len(sorted_blocks), " blocks:",
    #       directly_matchable_volume_on_focus_pair // 10**18)
    # print("Non-matchable weth in ", len(sorted_blocks), " blocks:",
    #       non_matchable_volume_on_focus_pair // 10**18)
    # if (directly_matchable_volume_on_focus_pair+non_matchable_volume_on_focus_pair > 0):
    #     print("Percentable of matchable weth in ", len(sorted_blocks), " blocks", directly_matchable_volume_on_focus_pair /
    #           (directly_matchable_volume_on_focus_pair+non_matchable_volume_on_focus_pair))
    investigated_time_period_in_sec = len(sorted_blocks) * 15
    print("Total surplus in USD for 30 days would be",
          directly_matchable_volume_on_focus_pair // 10**18 * 0.003 * 24*60*60 / investigated_time_period_in_sec * 30*eth_price, "plus saved slippage")
    print("That would imply an on-average price improvement of",
          0.3 * directly_matchable_volume_on_focus_pair / (directly_matchable_volume_on_focus_pair+non_matchable_volume_on_focus_pair), "[%]")
