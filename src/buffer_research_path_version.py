import pickle

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
from math import log
import csv

# This function counts the saved interactions against AMM of the same type in a batch due to cow or unidirectional cows:
# I.e., if there are two interactions against uniV3 in the same token pair, then 1 is saved via batching
# but if there is one interaction against univ2 and one against univ3 in the same token pair, we don't
# save any amm interaction, as both pools might be needed to reduce slippage.


def count_number_of_amm_interactions_due_to_opp_cow_or_unidirectional_cows(df):
    number_of_trades_per_pair = df.groupby(
        ["token_a_address", "token_b_address", "project", "version"]).size()
    number_of_saved_trades_per_pair = dict()
    for index_set, count in number_of_trades_per_pair.items():
        t, f, protocol, version = index_set
        if t < f:
            t, f = f, t
        if (t, f, protocol, version) in number_of_saved_trades_per_pair:
            number_of_saved_trades_per_pair[t, f, protocol, version] += count
        else:
            number_of_saved_trades_per_pair[t,
                                            f, protocol, version] = count - 1

    return sum(number_of_saved_trades_per_pair.values())


def apply_batch_trades_on_buffer_and_account_trade_statistic(sent_volume_per_pair, buffers, buffer_allow_listed_tokens):
    nr_of_external_trades = 0
    nr_of_internal_trades = 0
    sum_unmatched_vol = 0
    sum_matched_vol = 0
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
        unmatched_cow_vol = sent_vol_from_to - sent_vol_to_from
        matched_cow_vol = 2*min(sent_vol_from_to,
                                sent_vol_to_from)
        # only use buffers, if both tokens are in the allowlist and buffer is sufficient to cover the trade
        if f not in buffer_allow_listed_tokens or t not in buffer_allow_listed_tokens:
            sum_matched_vol += matched_cow_vol
            sum_unmatched_vol += unmatched_cow_vol
            nr_of_external_trades += 1
        elif buffers[t] < unmatched_cow_vol:
            sum_matched_vol += matched_cow_vol + buffers[t]
            sum_unmatched_vol += unmatched_cow_vol - buffers[t]
            buffers[f] += buffers[t]
            buffers[t] = 0
            nr_of_external_trades += 1
        else:
            buffers[f] += unmatched_cow_vol
            buffers[t] -= unmatched_cow_vol
            sum_matched_vol += unmatched_cow_vol + matched_cow_vol
            nr_of_internal_trades += 1

    # simple sanity checks
    assert abs(previous_buffer_usd_value -
               sum(buffers.values())) <= 1
    assert all(v >= 0 for v in buffers.values())
    return buffers, sum_unmatched_vol, nr_of_internal_trades, nr_of_external_trades, sum_matched_vol


def compute_buffer_evolution(df_sol, init_buffers, buffer_allow_listed_tokens):
    buffers_across_time = []
    external_vol_across_time = []
    internally_matched_vol_across_time = []
    buffers = init_buffers
    nr_of_external_trades_across_time = []
    nr_of_internal_trades_across_time = []

    def update_buffers(batch_df):
        nonlocal buffers
        nr_of_additional_internal_trades_from_batching = count_number_of_amm_interactions_due_to_opp_cow_or_unidirectional_cows(
            batch_df.copy())
        sent_volume_per_pair = batch_df.groupby(
            ["token_a_address", "token_b_address"]).usd_amount.sum().to_dict()
        updated_buffers, external_vol, nr_of_internal_trades_in_batch, nr_of_external_trades_in_batch, matched_vol = apply_batch_trades_on_buffer_and_account_trade_statistic(
            sent_volume_per_pair, buffers, buffer_allow_listed_tokens)
        nr_of_external_trades_across_time.append(
            nr_of_external_trades_in_batch)
        nr_of_internal_trades_across_time.append(
            nr_of_internal_trades_in_batch+nr_of_additional_internal_trades_from_batching)
        external_vol_across_time.append(external_vol)
        internally_matched_vol_across_time.append(matched_vol)
        buffers_across_time.append(updated_buffers.copy())
        buffers.update(updated_buffers)

    df_sol.groupby("block_number").apply(update_buffers)
    df = pd.DataFrame.from_records(buffers_across_time)
    df['internally_matched_vol_across_time'] = internally_matched_vol_across_time
    df["external_vol_across_time"] = external_vol_across_time
    df["nr_of_internal_trades"] = nr_of_internal_trades_across_time
    df["nr_of_external_trades"] = nr_of_external_trades_across_time
    return df


if __name__ == '__main__':

    fetch_data_from_dune = True
    verbose_logging = False

    if fetch_data_from_dune:
        dune_connection = DuneAnalytics.new_from_environment()
        dune_data = dune_connection.fetch(
            query_filepath="./src/dune_api/queries/dex_ag_path_trades.sql",
            network='mainnet',
            name="dex ag trades",
            parameters=[]
        )
        with open("data/dune_trading_data_download.txt", "bw+") as f:
            pickle.dump(dune_data, f, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open("data/dune_trading_data_download.txt", "rb") as f:
            dune_data = pickle.load(f)

    df = pd.DataFrame.from_records(dune_data)
    # We are sorting out all trades that don't have prices. This number of trades is neglegible for the overall analysis
    df = df[df['usd_amount'].notna()]
    # scaling factor to scale revenue for 1 year
    revenue_scaling_factor = 365*24*60*60 / \
        ((df['block_number'].max() - df['block_number'].min()) * 13.5)

    # preparations for writing simulation results into csv file
    result_file = 'result.csv'
    header = ['Buffer Investment [USD]',
              'AllowListed Buffer-Tokens',
              'Ratio Internal AMM interactions vs External AMM trade',
              'Ratio Internally Matched Vol',
              'Fee Revenue [USD/Year]',
              'Ratio of gas costs vs normal dex agg execution'
              'Ratio of gas costs vs normal dex agg execution(with optimized smart contract)'
              ]
    with open(result_file, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
    # gas costs rough estimations
    gas_cost_avg_amm_trade = 200000
    gas_cost_avg_amm_interaction = 75000

    gas_cost_over_head_cowswap = 90000
    gas_cost_internal_trade = 130000
    gas_cost_over_head_cowswap_in_potential_v2 = 55000
    gas_cost_internal_trade_in_potential_v2 = 105000
    number_of_distinct_trades = len(df.copy()['tx_hash'].unique())
    print(number_of_distinct_trades)

    # Starting simulation with different model parameters
    for initial_buffer_value_in_usd in [1_300_000, 10_000_000, 50_000_000]:
        for trade_activity_threshold_for_buffers_to_be_funded in [0.01, 0.005, 0.0002, 0]:

            tokens = set.union({t for t in df['token_a_address']}, {
                t for t in df['token_b_address']})

            normalized_token_appearance_counts = df['token_b_address'].value_counts(
                normalize=True)
            buffer_allow_listed_tokens = list({
                t for t in tokens
                if t in normalized_token_appearance_counts and
                normalized_token_appearance_counts[t] >= trade_activity_threshold_for_buffers_to_be_funded
            })
            if trade_activity_threshold_for_buffers_to_be_funded == 0:
                # let's simulate our current situation in the buffers 1.3M distributed over 1500 tokens
                buffers = {t: initial_buffer_value_in_usd /
                           1500 if t in buffer_allow_listed_tokens else 0 for t in tokens}
            else:
                buffers = {t: initial_buffer_value_in_usd /
                           len(buffer_allow_listed_tokens) if t in buffer_allow_listed_tokens else 0 for t in tokens}
            if len(buffer_allow_listed_tokens) > 0:
                initial_buffer_value_per_token_in_usd = initial_buffer_value_in_usd / \
                    len(buffer_allow_listed_tokens)
            else:
                initial_buffer_value_per_token_in_usd = 0

            result_df = compute_buffer_evolution(
                df, buffers, buffer_allow_listed_tokens)
            saved_internal_trades = result_df['nr_of_internal_trades'].sum(
            )
            ratio_internal_trades = round(
                saved_internal_trades/(saved_internal_trades+result_df['nr_of_external_trades'].sum()), 4)

            # result array corresponds to the header defined above
            internally_traded_volume = round(result_df['internally_matched_vol_across_time'].sum(
            ))
            externally_traded_volume = round(result_df['external_vol_across_time'].sum(
            ))
            result_array = [initial_buffer_value_in_usd,
                            len(buffer_allow_listed_tokens) if trade_activity_threshold_for_buffers_to_be_funded != 0 else 1500,
                            ratio_internal_trades,
                            round(internally_traded_volume /
                                  (externally_traded_volume + internally_traded_volume), 2),
                            round(revenue_scaling_factor *
                                  internally_traded_volume*0.0005, 2),
                            round((number_of_distinct_trades * (gas_cost_over_head_cowswap + gas_cost_avg_amm_trade) - saved_internal_trades *
                                  gas_cost_avg_amm_interaction)/(number_of_distinct_trades * gas_cost_avg_amm_trade), 2),
                            round((number_of_distinct_trades * (gas_cost_over_head_cowswap_in_potential_v2 + gas_cost_avg_amm_trade) - saved_internal_trades *
                                  gas_cost_avg_amm_interaction)/(number_of_distinct_trades * gas_cost_avg_amm_trade), 2)]

            with open(result_file, 'a', encoding='UTF8') as f:
                writer = csv.writer(f)
                writer.writerow(result_array)
