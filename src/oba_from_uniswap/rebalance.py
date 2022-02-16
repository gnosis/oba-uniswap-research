from networkx import DiGraph, network_simplex
import pandas as pd
from .common import get_dfs, get_block_data_file, get_prices_at_blocks
from math import log

EPS = 1e-5
TOL = 1e-3

# Compute matched volume in general order graph
# sent_vol is a dict (token_from, token_to) -> volume
# returns (token_from, token_to) -> matched_volume
def compute_matched_vol_per_pair(sent_vol):
    d = DiGraph()
    d.add_weighted_edges_from((f, t, -1) for f, t in sent_vol.keys())
    for ft, vol in sent_vol.items():
        f, t = ft
        d[f][t]['capacity']=sent_vol[f, t]
    r = network_simplex(d)
    for f, t in sent_vol.keys():
        assert r[1][f][t] <= sent_vol[f, t] + EPS
    return {(f, t): r[1][f][t] for f, t in sent_vol.keys()}

# Compute matched volume in general order graph
# sent_vol is a dict (token_from, token_to) -> volume
def compute_matched_vol(sent_vol):
    return sum(compute_matched_vol_per_pair(sent_vol).values())

def compute_token_balance_delta(sent_vol):
    matched_vol = compute_matched_vol_per_pair(sent_vol)
    tokens = {k[0] for k in sent_vol.keys()} | {k[1] for k in sent_vol.keys()}
    imbalances = {t: 0 for t in tokens}
    for (f, t) in matched_vol.keys():
        imbalances[t] -= sent_vol[f, t] - matched_vol[f, t]
        imbalances[f] += sent_vol[f, t] - matched_vol[f, t]
    return imbalances

# Move volume from most filled buffers to cover all negative buffers
# Pre: sum of buffers >= 0 (buffers are rebalanceable)
def rebalance_buffers_shave(buffers):
    init_size = sum(buffers.values())
    assert init_size >= 0

    # sort buffers by decreasing vol
    buffers = dict(sorted(
        buffers.items(), key=lambda kv: kv[1], reverse=True
    ))
    tokens_decr = list(buffers.keys())
    nr_buffers = len(buffers)
    index_first_buffer_with_nonpos_vol = next(
        (bi for bi in range(nr_buffers) if buffers[tokens_decr[bi]]<=0),
        None
    )
    if index_first_buffer_with_nonpos_vol == None:
        return
    total_neg_vol = sum(buffers[t] for t in tokens_decr[index_first_buffer_with_nonpos_vol:])
    moved_vol = 0
    for bi in range(index_first_buffer_with_nonpos_vol):
        if -total_neg_vol - moved_vol <= 0:
            break
        t = tokens_decr[bi]
        next_t = tokens_decr[bi + 1]
        max_moveable_vol = buffers[t] - max(0, buffers[next_t])
        total_vol_to_move = min(max_moveable_vol * (bi + 1), -total_neg_vol - moved_vol)
        assert total_vol_to_move >= 0
        vol_to_move_from_each_prev_buffer = total_vol_to_move / (bi + 1)
        for bj in range(0, bi + 1):
            buffers[tokens_decr[bj]] -= vol_to_move_from_each_prev_buffer
        moved_vol += total_vol_to_move

    for bi in range(index_first_buffer_with_nonpos_vol, nr_buffers):
        buffers[tokens_decr[bi]] = 0

    if abs(log(init_size) - log(sum(buffers.values()))) >= TOL:
        print(init_size, sum(buffers.values()))
    assert abs(log(init_size) - log(sum(buffers.values()))) <= TOL   # buffer conservation
    return buffers

total_matched_vol = 0
total_unmatched_vol = 0
total_rebalanced_vol = 0

# Computes the final token balances for a batch, rebalancing the buffers to
# keep them non negative if needed. 
# Args:
#   - sent_vol: dict (f, t)->volume, with the volume sent from token f to token t
#   - buffers: dict t->volume, with the volume of token t in the buffer
# Returns:
#   - updated buffers, and total volume rebalanced
def compute_token_balance_delta_constant_buffers_simple(sent_vol, buffers):
    #print(sent_vol, buffers)
    matched_vol = compute_matched_vol_per_pair(sent_vol)
    global total_matched_vol
    global total_unmatched_vol
    global total_rebalanced_vol
    total_matched_vol += sum(matched_vol.values())
    buffer_size = sum(buffers.values())
    rebalanced_vol = 0
    for (f, t) in matched_vol.keys():
        unmatched_vol = sent_vol[f, t] - matched_vol[f, t]
        total_unmatched_vol += unmatched_vol
        # do not use internal buffers for this token pair if there is 
        # not enough buffer in total that could be moved.
        if unmatched_vol > buffer_size:
            rebalanced_vol += unmatched_vol
            continue
        buffers[t] -= unmatched_vol
        buffers[f] += unmatched_vol
        if buffers[t] < 0:
            rebalance_vol = -buffers[t]
            buffers = rebalance_buffers_shave(buffers)
            rebalanced_vol += rebalance_vol
    assert abs(log(buffer_size) - log(sum(buffers.values()))) <= TOL
    assert all(v >= -EPS for v in buffers.values())
    total_rebalanced_vol += rebalanced_vol
    return buffers, rebalanced_vol


# Improves on the above by first depositing all sold volume into
# buffers only computing rebalances afterwards.
def compute_token_balance_delta_constant_buffers(sent_vol, buffers):
    matched_vol = compute_matched_vol_per_pair(sent_vol)
    buffer_size = sum(buffers.values())
    rebalanced_vol = 0
    # First deposit all sold volume into buffers
    for (f, t) in matched_vol.keys():
        unmatched_vol = sent_vol[f, t] - matched_vol[f, t]
        # do not use internal buffers for this token pair if there is 
        # not enough buffer in total that could be moved.
        if unmatched_vol > buffer_size:
            rebalanced_vol += unmatched_vol
            continue
        buffers[f] += unmatched_vol
    # Then remove buy amounts and rebalance as necessary.
    for (f, t) in matched_vol.keys():
        unmatched_vol = sent_vol[f, t] - matched_vol[f, t]
        # do not use internal buffers for this token pair if there is 
        # not enough buffer in total that could be moved.
        if unmatched_vol > buffer_size:
            continue
        buffers[t] -= unmatched_vol
        if buffers[t] < 0:
            rebalance_vol = -buffers[t]
            buffers = rebalance_buffers_shave(buffers)
            rebalanced_vol += rebalance_vol
    assert abs(log(buffer_size) - log(sum(buffers.values()))) <= TOL
    assert all(v >= 0 for v in buffers.values())
    return buffers, rebalanced_vol


#s={('FARM', 'WBTC'): 3307.385261238168, ('UNI', 'FARM'): 371.86397130752226, ('WETH', 'FARM'): 797.0731743335803, ('WETH', 'USDT'): 602.1090415509582}
#b={'WETH': 1000000, 'DAI': 0, 'USDT': 0, 'FARM': 0, 'LINK': 0, 'UNI': 0, 'AMPL': 0, 'YFI': 0, 'WBTC': 0, 'USDC': 0, 'CEL': 0, 'RAMP': 0, 'PICKLE': 0, 'KORE': 0, 'CORE': 0}
#compute_token_balance_delta_constant_buffers_simple(s,b)

def adjust_buffer_vol_from_prices(buffers, prices_at_previous_update, current_prices):
    for t in buffers.keys():
        buffers[t] = buffers[t]/prices_at_previous_update[t] * current_prices[t]

def compute_buffers_constant(df_sol, init_buffers, prices_in_eth):    
    buffers_across_time = []
    rebalanced_vol_across_time = []
    buffers = init_buffers
    prev_block = None
    def update_buffers(batch_df):
        nonlocal buffers
        nonlocal prev_block
        cur_block = batch_df.iloc[0].block
        if prev_block is not None:
            adjust_buffer_vol_from_prices(buffers, prices_in_eth[prev_block], prices_in_eth[cur_block])
        prev_block = cur_block
        sent_vol = batch_df.groupby(["sell_token","buy_token"]).max_vol_eth.sum().to_dict()
        updated_buffers, rebalanced_vol = compute_token_balance_delta_constant_buffers_simple(sent_vol, buffers)
        rebalanced_vol_across_time.append(rebalanced_vol)
        buffers_across_time.append(updated_buffers)
        buffers.update(updated_buffers)
    df_sol.groupby("batch_start_time").apply(update_buffers)
    df = pd.DataFrame.from_records(buffers_across_time)
    df["rebalanced_vol"] = rebalanced_vol_across_time
    return df

if __name__ == "__main__":
    DATA_PATH='data/oba_from_uniswap/instances-11827625-11874424'
    df_exec = get_block_data_file(f'{DATA_PATH}/random_sample-1000', 60, 15, "0.99", "0.01")
    tokens = pd.concat([df_exec.sell_token, df_exec.buy_token]).unique()
    prices_in_eth = get_prices_at_blocks(DATA_PATH, df_exec.block.unique().tolist(), tokens)

    df=df_exec
    df['batch_start_time'] = df['timestamp'].floordiv(60)

    init_buffer_size = 10
    buffers = {t: init_buffer_size/len(tokens) for t in tokens}
    compute_buffers_constant(df, buffers, prices_in_eth)