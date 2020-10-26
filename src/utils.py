def generate_focus_pairs(sorted_blocks, swaps_by_block):
    focus_pairs = [
        [o['sellToken'], o['buyToken']]
        for block in sorted_blocks[1:-1]
        for o in swaps_by_block.get(block, [])
    ]
    return list({(tuple(t)) for t in focus_pairs})


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


def plot_match_survivor(results, filename=None):
    """If filename is not None then creates file on disk."""
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(
        results.values(),
        bins=20,
        density=False,
        cumulative=-1,
        log=True
    )
    plt.title('Number of matchable pairs')
    plt.xlabel('x')
    plt.ylabel('Nr pairs with probability of match >= x')
    if filename is not None:
        plt.savefig(filename)
    plt.show()
