def is_there_a_opposite_match_in_next_k_blocks(startblock, k, focus_pair,
                                               swaps_by_block, sorted_blocks):
    for j in range(startblock, startblock + k):
        for o in swaps_by_block.get(sorted_blocks[j], []):
            if focus_pair is not None:
                if focus_pair[0] == o['buyToken'] and \
                        focus_pair[1] == o['sellToken']:
                    return True
    return False
