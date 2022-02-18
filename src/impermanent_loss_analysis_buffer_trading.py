import datetime

import numpy as np
import matplotlib.pyplot as plt

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
import csv


pd.set_option('display.max_rows', 50)
pd.set_option("display.min_rows", 49)

TOL = 1e-7


def apply_batch_trades_on_buffer_and_account_trade_statistic(sent_volume_per_pair, buffers, buffer_allow_listed_tokens):
    sum_rebalance_vol = 0
    sum_matched_vol = 0

    pair_visited = {}
    for (f, t) in list(sent_volume_per_pair):
        # don't visit a pair twice
        if (f, t) in pair_visited:
            continue
        else:
            pair_visited[f, t] = True
            pair_visited[t, f] = True

        # calculate the volumes sent and received on pair
        sent_tokens_from_to = sent_volume_per_pair[f, t]["token_b_amount_raw"]
        if not (t, f) in sent_volume_per_pair:
            sent_volume_per_pair[t, f] = {
                'token_a_amount_raw': 0, 'token_b_amount_raw': 0}
            sent_tokens_to_from = 0
        else:
            sent_tokens_to_from = sent_volume_per_pair[t,
                                                       f]["token_a_amount_raw"]
        if sent_tokens_from_to < sent_tokens_to_from:
            f, t = t, f
            sent_tokens_from_to = sent_volume_per_pair[
                f, t]["token_b_amount_raw"]
            sent_tokens_to_from = sent_volume_per_pair[
                t, f]["token_a_amount_raw"]
        # calculate the matched amounts
        unmatched_cow_vol = sent_tokens_from_to - sent_tokens_to_from
        matched_cow_vol = 2*min(sent_tokens_from_to,
                                sent_tokens_to_from)

        # only use buffers, if both tokens are in the allowlist and buffer is sufficient to cover the trade
        # can be optimized later to use also for partial matches against the buffer
        if f not in buffer_allow_listed_tokens or t not in buffer_allow_listed_tokens or buffers[t] < unmatched_cow_vol:
            sum_matched_vol += matched_cow_vol
            sum_rebalance_vol += unmatched_cow_vol
        else:
            buffers[f] += sent_volume_per_pair[f, t]["token_a_amount_raw"] - sent_volume_per_pair[t,
                                                                                                  f]["token_b_amount_raw"]
            buffers[t] -= unmatched_cow_vol
            sum_matched_vol += unmatched_cow_vol + matched_cow_vol

    # simple sanity checks
    assert all(v >= -0.1 for v in buffers.values())
    return buffers


def compute_buffer_evolution(df_sol, init_buffers, buffer_allow_listed_tokens):
    buffers_across_time = []
    buffers = init_buffers

    def update_buffers(batch_df):
        nonlocal buffers
        agg_sent_volume_per_pair = batch_df.groupby(
            ["token_a_address", "token_b_address"], as_index=True).agg({"token_a_amount_raw": "sum", "token_b_amount_raw":  "sum"}).to_dict('index')
        updated_buffers = apply_batch_trades_on_buffer_and_account_trade_statistic(
            agg_sent_volume_per_pair, buffers, buffer_allow_listed_tokens)
        buffers_across_time.append(updated_buffers.copy())
        buffers.update(updated_buffers)

    df_sol.groupby("block_number").apply(update_buffers)
    df = pd.DataFrame.from_records(buffers_across_time)
    return df


if __name__ == '__main__':

    initial_buffer_value_in_usd = 10_000_000
    trade_activity_threshold_for_buffers_to_be_funded = 0.001

    # preparations for writing simulation results into csv file
    result_file = 'result_impermantent_loss_simulation.csv'
    header = ['Date',
              'HODL Strategy [USD]',
              'Cow Buffer Strategy [USD]',
              'Ratio'
              ]
    with open(result_file, '+w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
    numdays = 5
    base = datetime.datetime.today()
    initial_buffer = {}
    buffer_allow_listed_tokens = {}
    approximated_prices_by_previous_prices = {}
    buffers = {}
    for date_time in [base - datetime.timedelta(days=numdays-x) for x in range(numdays)]:
        print(date_time)
        dune_connection = DuneAnalytics.new_from_environment()
        dune_trading_data = dune_connection.fetch(
            query_filepath="./src/dune_api/queries/dex_ag_trades_within_time_frame.sql",
            network='mainnet',
            name="dex ag trades",
            parameters=[
                {
                    "key": "start_time",
                    "type": "text",
                    "value": f'\'{date_time.strftime("%Y-%m-%d %H:%M")}\'',
                },
                {
                    "key": "end_time",
                    "type": "text",
                    "value": f'\'{(date_time+datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M")}\'',
                }
            ]
        )
        df = pd.DataFrame.from_records(dune_trading_data)
        token_prices_in_usd = dune_connection.fetch(
            query_filepath="./src/dune_api/queries/prices_from_start_and_end_of_day.sql",
            network='mainnet',
            name="tokens and prices",
            parameters=[
                {
                    "key": "start_time",
                    "type": "text",
                    "value": f'\'{date_time.strftime("%Y-%m-%d %H:%M")}\'',
                },
                {
                    "key": "end_time",
                    "type": "text",
                    "value": f'\'{(date_time+datetime.timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M")}\'',
                }
            ]
        )
        token_prices_in_usd = {o['token']: (o['usd_price'], o['decimals'])
                               for o in token_prices_in_usd}

        if not bool(initial_buffer):
            # procedure to be run only once during initialization
            tokens = set.union({t for t in df['token_a_address'] if t in token_prices_in_usd}, {
                t for t in df['token_b_address'] if t in token_prices_in_usd})

            normalized_token_appearance_counts = df['token_b_address'].value_counts(
                normalize=True)
            approximated_prices_by_previous_prices = token_prices_in_usd
            buffer_allow_listed_tokens = list({t for t in tokens if (t in normalized_token_appearance_counts
                                                                     and normalized_token_appearance_counts[t] > trade_activity_threshold_for_buffers_to_be_funded)})
            initial_buffer = {t:  (initial_buffer_value_in_usd /
                              len(buffer_allow_listed_tokens) * pow(10, 18) / token_prices_in_usd[t][0]) if t in buffer_allow_listed_tokens else 0 for t in tokens if t in token_prices_in_usd}
            buffers = initial_buffer.copy()

        approximated_prices_by_previous_prices = {
            t:  token_prices_in_usd[t] if t in token_prices_in_usd else approximated_prices_by_previous_prices[t] for t in tokens}
        df = df[df['token_b_address'].isin(tokens)]
        df = df[df['token_a_address'].isin(tokens)]
        result_df = compute_buffer_evolution(
            df, buffers, buffer_allow_listed_tokens)

        hodl_value = sum([approximated_prices_by_previous_prices[t][0] * initial_buffer[t] / pow(10, 18)
                         for t in initial_buffer])
        cow_buffer_value = sum(
            [approximated_prices_by_previous_prices[t][0] * buffers[t] / pow(10, 18)
             for t in buffers])
        result_array = [date_time.strftime("%Y-%m-%d"),
                        hodl_value,
                        cow_buffer_value,
                        cow_buffer_value / hodl_value
                        ]

        with open(result_file, 'a', encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(result_array)

    # initial starting values:
    dates = [(datetime.datetime.today() -
              datetime.timedelta(days=numdays+1)).strftime("%Y-%m-%d")]
    hodl = [initial_buffer_value_in_usd]
    buffer_value = [initial_buffer_value_in_usd]
    ratio = [1]
    # reading stored values
    with open(result_file, 'r', encoding='UTF8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            dates.append(row[0])
            hodl.append(float(row[1]))
            buffer_value.append(float(row[2]))
            ratio.append(float(row[3]))
    # plotting
    x_axis = dates
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(x_axis, buffer_value, 'b', label='BUFFER strategy')
    ax1.plot(x_axis, hodl, 'r', label='HODL strategy')
    ax1.legend(loc=0)
    ax2.plot(x_axis, ratio, 'y',  label='Ratio strategies')
    ax2.legend(loc=1)
    ax1.set_xlabel('Date')
    plt.xticks(x_axis[::5])
    plt.show()
