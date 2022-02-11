from src.oba_from_uniswap.common import get_block_data_file, get_prices_at_blocks
import pickle

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
from math import log
import csv


pd.set_option('display.max_rows', 50)
pd.set_option("display.min_rows", 49)

TOL = 1e-7

total_matched_vol = 0
total_unmatched_vol = 0
total_rebalanced_vol = 0


def count_number_of_saved_trades_due_to_cow(df):
    number_of_trades_per_pair = df.groupby(
        ["token_a_address", "token_b_address"]).size()
    number_of_saved_trades_per_pair = dict()
    for tf, count in number_of_trades_per_pair.items():
        t, f = tf
        if t < f:
            t, f = f, t
        if (t, f) in number_of_saved_trades_per_pair:
            number_of_saved_trades_per_pair[t, f] += count
        else:
            number_of_saved_trades_per_pair[t, f] = count - 1
    return sum(
        number_of_saved_trades_per_pair.values())


def apply_batch_trades_on_buffer_and_account_trade_statistic(sent_volume_per_pair, buffers, buffer_allow_listed_tokens):
    global total_matched_vol
    global total_unmatched_vol
    global total_rebalanced_vol
    nr_of_external_trades = 0
    nr_of_internal_trades = 0
    rebalanced_vol = 0
    previous_buffer_usd_value = sum(buffers.values())

    pair_visited = {}
    for (f, t) in sent_volume_per_pair.keys():
        # don't visit a pair twice
        if (f, t) in pair_visited:
            continue
        else:
            pair_visited[f, t] = True
            pair_visited[t, f] = True

        # calculate the volumes sent and received on pair
        sent_vol_from_to = sent_volume_per_pair[f, t]
        if not (t, f) in sent_volume_per_pair:
            sent_vol_to_from = 0
        else:
            sent_vol_to_from = sent_volume_per_pair[t, f]
        if sent_vol_from_to < sent_vol_to_from:
            f, t = t, f
            sent_vol_from_to, sent_vol_to_from = sent_vol_to_from, sent_vol_from_to
        # calculate the matched amounts
        unmatched_vol = sent_vol_from_to - sent_vol_to_from
        total_matched_vol += min(sent_vol_from_to, sent_vol_to_from)
        total_unmatched_vol += unmatched_vol
        # only use buffers, if both tokens are in the allowlist and buffer is sufficient to cover the trade
        if not f in buffer_allow_listed_tokens or not t in buffer_allow_listed_tokens or buffers[t] < unmatched_vol:
            rebalanced_vol += unmatched_vol
            nr_of_external_trades += 1
        else:
            buffers[f] += unmatched_vol
            buffers[t] -= unmatched_vol
            nr_of_internal_trades += 1

    # simple sanity checks
    assert abs(previous_buffer_usd_value -
               sum(buffers.values())) <= 1
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
        if prev_block is not None and cur_block in prices:
            adjust_buffer_vol_from_prices(
                buffers, prices[prev_block], prices[cur_block])
            prev_block = cur_block
        nr_of_additional_internal_trades_from_batching = count_number_of_saved_trades_due_to_cow(
            batch_df.copy())
        sent_volume_per_pair = batch_df.groupby(
            ["token_a_address", "token_b_address"]).usd_amount.sum().to_dict()
        updated_buffers, rebalanced_vol, nr_of_internal_trades_in_batch, nr_of_rebalances_in_batch = apply_batch_trades_on_buffer_and_account_trade_statistic(
            sent_volume_per_pair, buffers, buffer_allow_listed_tokens)
        nr_of_external_trades.append(nr_of_rebalances_in_batch)
        nr_of_internal_trades.append(
            nr_of_internal_trades_in_batch+nr_of_additional_internal_trades_from_batching)
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

    fetch_data_from_dune = False
    verbose_logging = False

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
    # print(df)

    header = ['Investment [USD]', 'AllowListed Tokens for Buffers',
              'Ratio internal trade numbers', 'Avg. estimated gas saving of internal trade vs normal AMM trade', 'Gas saving compared to normal AMM trade', 'Potential gas saving (smart contract v2) compared to normal AMM trade']
    gas_cost_avg_amm_trade = 200000
    gas_cost_over_head_cowswap = 90000
    gas_cost_internal_trade = 130000
    gas_cost_over_head_cowswap_in_potential_v2 = 50000
    gas_cost_internal_trade_in_potential_v2 = 105000
    with open('result.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)

    # Starting simulation with different model parameters
    for initial_buffer_value_in_usd in [1_000_000, 10_000_000, 50_000_000]:
        for trade_activity_threshold_for_buffers_to_be_funded in [0.01, 0.001, 0.0005, 0]:

            token_prices = token_prices_in_usd
            tokens = set.union({t for t in df['token_a_address']}, {
                t for t in df['token_b_address']})

            value_counts = df['token_b_address'].value_counts(normalize=True)
            buffer_allow_listed_tokens = list({t for t in tokens if (t in value_counts
                                                                     and value_counts[t] > trade_activity_threshold_for_buffers_to_be_funded)})
            buffers = {t: t in buffer_allow_listed_tokens and initial_buffer_value_in_usd /
                       len(buffer_allow_listed_tokens) or 0 for t in tokens}

            if verbose_logging:
                print(
                    "--------------------------Experiment setup----------------------------")
                print("There will be ", len(
                    tokens), " tokens traded in the dex-aggregator trading data set")
                print("Buffer is only allowed in ", len(
                    buffer_allow_listed_tokens), "tokens")
                print("Total buffer investment", initial_buffer_value_in_usd,
                      "[USD] and in each token there is a buffer of:", initial_buffer_value_in_usd/len(buffer_allow_listed_tokens), "[USD]")
                print(
                    "---------------------------------------------------------------------")

            result_df = compute_buffer_evolution(
                df, buffers, token_prices, buffer_allow_listed_tokens)
            ratio_internal_trades = result_df['nr_of_internal_trades'].sum(
            )/(result_df['nr_of_internal_trades'].sum()+result_df['nr_of_external_trades'].sum())
            if verbose_logging:
                print(
                    "-------------------------------Result--------------------------------")
                print("Ratio of internal trades in comparison to total trades",
                      ratio_internal_trades)
                print("Total number of internal and external trades considered",
                      (result_df['nr_of_internal_trades'].sum()+result_df['nr_of_external_trades'].sum()))
                print(
                    "-------------------------------Compressed result--------------------------------")
            data = [initial_buffer_value_in_usd, len(
                buffer_allow_listed_tokens), ratio_internal_trades, gas_cost_internal_trade/gas_cost_avg_amm_trade, (gas_cost_internal_trade*ratio_internal_trades + (gas_cost_over_head_cowswap+gas_cost_avg_amm_trade)*(1-ratio_internal_trades))/gas_cost_avg_amm_trade, (gas_cost_internal_trade_in_potential_v2*ratio_internal_trades + (gas_cost_over_head_cowswap_in_potential_v2+gas_cost_avg_amm_trade)*(1-ratio_internal_trades))/gas_cost_avg_amm_trade]
            print(data)
            with open('result.csv', 'a', encoding='UTF8') as f:
                writer = csv.writer(f)
                writer.writerow(data)
