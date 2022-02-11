from src.oba_from_uniswap.common import get_block_data_file, get_prices_at_blocks
import pickle

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
from math import log


pd.set_option('display.max_rows', 50)
pd.set_option("display.min_rows", 49)

TOL = 1e-7

total_matched_vol = 0
total_unmatched_vol = 0
total_rebalanced_vol = 0


def compute_token_balance_delta_constant_buffers_simple(sent_vol, buffers, buffer_allow_listed_tokens):
    # print("sent vol", sent_vol)
    # print("buffers", buffers)
    nr_of_external_trades = 0
    nr_of_internal_trades = 0
    global total_matched_vol
    global total_unmatched_vol
    global total_rebalanced_vol
    buffer_size = sum(buffers.values())
    rebalanced_vol = 0
    initial_keys = list(sent_vol.keys())
    for (f, t) in initial_keys:
        if not (t, f) in sent_vol:
            sent_vol[t, f] = 0
    for (f, t) in sent_vol.keys():
        if t < f:
            continue
        if sent_vol[f, t] < sent_vol[t, f]:
            tmp = f
            f = t
            t = tmp
        unmatched_vol = sent_vol[f, t] - sent_vol[t, f]
        total_matched_vol += min(sent_vol[f, t], sent_vol[t, f])
        total_unmatched_vol += unmatched_vol
        # do not use internal buffers for this token pair if there is
        # not enough buffer in total that could be moved.
        if not f in buffer_allow_listed_tokens:
            rebalanced_vol += unmatched_vol
            nr_of_external_trades += 1
            continue
        if buffers[t] < unmatched_vol:
            rebalanced_vol += unmatched_vol
            nr_of_external_trades += 1
        else:
            buffers[f] += unmatched_vol
            buffers[t] -= unmatched_vol
            nr_of_internal_trades += 1

    # simple sanity checks
    assert abs(log(buffer_size) - log(sum(buffers.values()))) <= TOL
    assert all(v >= 0 for v in buffers.values())
    total_rebalanced_vol += rebalanced_vol
    return buffers, rebalanced_vol, nr_of_internal_trades, nr_of_external_trades


def adjust_buffer_vol_from_prices(buffers, prices_at_previous_update, current_prices):
    for t in buffers.keys():
        buffers[t] = buffers[t] / \
            prices_at_previous_update[t] * current_prices[t]


def compute_buffer_evolution(df_sol, init_buffers, prices, buffer_allow_listed_tokens):
    buffers_across_time = []
    rebalanced_vol_across_time = []
    buffers = init_buffers
    prev_block = None
    nr_of_external_trades = []
    nr_of_internal_trades = []

    def update_buffers(batch_df):
        nonlocal buffers
        nonlocal prev_block
        cur_block = batch_df.iloc[0].block_number
        if prev_block is not None:
            if cur_block in prices:
                adjust_buffer_vol_from_prices(
                    buffers, prices[prev_block], prices[cur_block])
                prev_block = cur_block
        sent_vol = batch_df.groupby(
            ["token_a_address", "token_b_address"]).usd_amount.sum().to_dict()
        updated_buffers, rebalanced_vol, nr_of_internal_trades_in_batch, nr_of_rebalances_in_batch = compute_token_balance_delta_constant_buffers_simple(
            sent_vol, buffers, buffer_allow_listed_tokens)
        nr_of_external_trades.append(nr_of_rebalances_in_batch)
        nr_of_internal_trades.append(nr_of_internal_trades_in_batch)

        rebalanced_vol_across_time.append(rebalanced_vol)
        buffers_across_time.append(updated_buffers.copy())
        buffers.update(updated_buffers)
    df_sol.groupby("block_number").apply(update_buffers)
    df = pd.DataFrame.from_records(buffers_across_time)
    df["rebalanced_vol"] = rebalanced_vol_across_time
    df["nr_of_internal_trades"] = nr_of_internal_trades
    df["nr_of_external_trades"] = nr_of_external_trades
    return df


if __name__ == '__main__':

    # Model parameters
    initial_buffer_value_in_usd = 10_800_000
    trade_activity_threshold_for_buffers_to_be_funded = 0.001
    fetch_data_from_dune = False

    if fetch_data_from_dune:
        dune_connection = DuneAnalytics.new_from_environment()
        dune_data = dune_connection.fetch(
            query_filepath="./src/dune_api/queries/dex_ag_trades_within_one_day.sql",
            network='mainnet',
            name="dex ag trades",
        )
        with open("data/dune_trading_data_download.txt", "bw+") as f:
            pickle.dump(dune_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        token_prices_in_usd = dune_connection.fetch(
            query_filepath="./src/dune_api/queries/tokens_and_prices.sql",
            network='mainnet',
            name="tokens and prices",
        )
        with open("data/dune_price_data_download.txt", "bw+") as f:
            pickle.dump(token_prices_in_usd, f,
                        protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open("data/dune_trading_data_download.txt", "rb") as f:
            dune_data = pickle.load(f)
        with open("data/dune_price_data_download.txt", "rb") as f:
            token_prices_in_usd = pickle.load(f)

    df = pd.DataFrame.from_records(dune_data)
    # We are sorting out all trades that don't have prices. This number of trades is neglegible for the overall analysis
    df = df[df['usd_amount'].notna()]
    print(df)

    token_prices = token_prices_in_usd
    tokens = set.union({t for t in df['token_a_address']}, {
        t for t in df['token_b_address']})

    value_counts = df['token_b_address'].value_counts(normalize=True)
    buffer_allow_listed_tokens = list({t for t in tokens if (t in value_counts
                                       and value_counts[t] > trade_activity_threshold_for_buffers_to_be_funded)})
    buffers = {t: t in buffer_allow_listed_tokens and initial_buffer_value_in_usd /
               len(buffer_allow_listed_tokens) or 0 for t in tokens}

    print("--------------------------Experiment setup----------------------------")
    print("There will be ", len(
        tokens), " tokens traded in the dex-aggregator trading data set")
    print("Buffer is only allowed in ", len(
        buffer_allow_listed_tokens), "tokens")
    print("Total buffer investment", initial_buffer_value_in_usd,
          "[USD] and in each token there is a buffer of:", initial_buffer_value_in_usd/len(buffer_allow_listed_tokens), "[USD]")
    print("---------------------------------------------------------------------")

    df = compute_buffer_evolution(
        df, buffers, token_prices, buffer_allow_listed_tokens)
    print(df)
    print("-------------------------------Result--------------------------------")
    print("Ratio of internal trades in comparison to total trades",
          df['nr_of_internal_trades'].sum()/(df['nr_of_internal_trades'].sum()+df['nr_of_external_trades'].sum()))
    print("Total number of internal and external trades considered",
          (df['nr_of_internal_trades'].sum()+df['nr_of_external_trades'].sum()))
