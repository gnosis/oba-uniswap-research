def find_order_in_block(
    block, focus_pair,
    swaps_by_block
):
    for o in swaps_by_block.get(block, []):
        if focus_pair[0] == o['buyToken'] and \
           focus_pair[1] == o['sellToken']:
            return True
    return False


def find_order_in_next_k_blocks(
    start_block_index, k, focus_pair,
    swaps_by_block, sorted_blocks
):
    assert focus_pair is not None
    for block_index in range(start_block_index, start_block_index + k):
        if find_order_in_block(sorted_blocks[block_index], focus_pair, swaps_by_block):
            return True
    return False
