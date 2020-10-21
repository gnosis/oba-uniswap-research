def is_there_a_opposite_match_in_next_k_blocks(startblock, k, focus_pair,
                                               swaps_by_block, sorted_blocks):
    for j in range(startblock, startblock + k):
        for o in swaps_by_block.get(sorted_blocks[j], []):
            if focus_pair is not None:
                if focus_pair[0] in o['buyToken'] and \
                    (focus_pair[1] in o['sellToken'] or
                     (len(focus_pair) >= 3 and focus_pair[2]
                         in o['sellToken']) or
                     (len(focus_pair) >=
                      4 and focus_pair[3] in o['sellToken'])
                     ):
                    return True
    return False
