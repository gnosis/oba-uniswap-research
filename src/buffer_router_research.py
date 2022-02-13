from src.oba_from_uniswap.common import get_block_data_file, get_prices_at_blocks
import pickle

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
from math import log
import csv


pd.set_option('display.max_rows', 50)
pd.set_option("display.min_rows", 4)

TOL = 1e-7

total_matched_vol = 0
total_unmatched_vol = 0
total_rebalanced_vol = 0


def apply_trades_on_buffer_and_account_trade_statistic(row, buffers, buffer_allow_listed_tokens):
    nr_of_external_trades = 0
    nr_of_internal_trades = 0
    sum_rebalance_vol = 0
    sum_matched_vol = 0

    t = row['token_a_address']
    f = row['token_a_address']
    sent_volume_usd = row['usd_amount']
    # only use buffers, if both tokens are in the allowlist and buffer is sufficient to cover the trade
    if not f in buffer_allow_listed_tokens or not t in buffer_allow_listed_tokens or buffers[t] < sent_volume_usd:
        sum_rebalance_vol += sent_volume_usd
        nr_of_external_trades += 1
    else:
        buffers[f] += sent_volume_usd
        buffers[t] -= sent_volume_usd
        sum_matched_vol += sent_volume_usd
        nr_of_internal_trades += 1

    # simple sanity checks
    assert all(v >= 0 for v in buffers.values())
    return buffers, sum_rebalance_vol, nr_of_internal_trades, nr_of_external_trades, sum_matched_vol


def adjust_buffer_values_with_prices(buffers, prices_at_previous_update, current_prices):
    for t in buffers.keys():
        if t in prices_at_previous_update and t in current_prices:
            buffers[t] = buffers[t] / \
                prices_at_previous_update[t] * current_prices[t]


def compute_buffer_evolution(df_sol, init_buffers, prices, buffer_allow_listed_tokens):
    buffers_across_time = []
    external_vol_across_time = []
    internally_matched_vol_across_time = []
    buffers = init_buffers
    blocks_with_prices = [x['block_number'] for x in prices]
    prices_per_block = {}
    for p in prices:
        block_nr = p['block_number']
        if block_nr in prices_per_block.keys():
            prices_per_block[block_nr] = {**prices_per_block[block_nr], **{
                p['token']: p['usd_price']}}
        else:
            prices_per_block[block_nr] = {p['token']: p['usd_price']}
    prev_block = min(blocks_with_prices)

    nr_of_external_trades = []
    nr_of_internal_trades = []

    def update_buffers(row):
        nonlocal buffers
        nonlocal prev_block

        cur_block = row['block_number']
        if cur_block != prev_block and cur_block in blocks_with_prices:
            adjust_buffer_values_with_prices(
                buffers, prices_per_block[prev_block], prices_per_block[cur_block])
            prev_block = cur_block
        updated_buffers, external_vol, nr_of_internal_trades_in_batch, nr_of_rebalances_in_batch, matched_vol = apply_trades_on_buffer_and_account_trade_statistic(
            row, buffers, buffer_allow_listed_tokens)
        nr_of_external_trades.append(nr_of_rebalances_in_batch)
        nr_of_internal_trades.append(
            nr_of_internal_trades_in_batch)
        external_vol_across_time.append(external_vol)
        internally_matched_vol_across_time.append(matched_vol)
        buffers_across_time.append(updated_buffers.copy())
        buffers.update(updated_buffers)

    df_sol.sort_values(by=['block_number'])
    # print(df_sol)
    df_sol.apply(update_buffers, axis=1)
    df = pd.DataFrame.from_records(buffers_across_time)
    df['internally_matched_vol_across_time'] = internally_matched_vol_across_time
    df["external_vol_across_time"] = external_vol_across_time
    df["nr_of_internal_trades"] = nr_of_internal_trades
    df["nr_of_external_trades"] = nr_of_external_trades
    print("buffer sum output ", sum(buffers.values()))

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
    # scaling factor to scale revenue for 1 year
    revenue_scaling_factor = 360*24*60*60 / \
        ((df['block_number'].max() - df['block_number'].min()) * 13.5)

    # preparations for writing simulation results into csv file
    result_file = 'result.csv'
    header = ['Buffer Investment [USD]',
              'AllowListed Buffer-Tokens',
              'Ratio Internal Trades',
              'Ratio Internally Matched Vol',
              'Fee Revenue [USD/Year]',
              'Gas Saving vs Normal Trade',
              'Gas Saving(in v2) vs Normal Trade',
              ]
    with open(result_file, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
    # gas costs rough estimations
    # this is the usual univ3 costs for 1 hop: cp https://etherscan.io/tx/0xef5ff4044b107208281266e8fc560031735fea8778c13d8d0535dc585ff94b41
    gas_cost_avg_amm_trade = 165000
    gas_cost_over_head_best_router = 200
    # tx intitation(20k) normal transfers (2*25k) + price provision (1k) + 1 signature verification (3k) + reading allowed price provider (2k)
    gas_cost_internal_trade = 80000

    # Starting simulation with different model parameters
    for initial_buffer_value_in_usd in [1_000_000, 10_000_000, 50_000_000]:
        for trade_activity_threshold_for_buffers_to_be_funded in [0.01, 0.001, 0.0005, 0]:

            token_prices = token_prices_in_usd
            tokens = set.union({t for t in df['token_a_address']}, {
                t for t in df['token_b_address']})

            normalized_token_appearance_counts = df['token_b_address'].value_counts(
                normalize=True)
            buffer_allow_listed_tokens = list({t for t in tokens if (t in normalized_token_appearance_counts
                                                                     and normalized_token_appearance_counts[t] > trade_activity_threshold_for_buffers_to_be_funded)})
            buffers = {t: t in buffer_allow_listed_tokens and initial_buffer_value_in_usd /
                       len(buffer_allow_listed_tokens) or 0 for t in tokens if t in buffer_allow_listed_tokens}

            if verbose_logging:
                print(
                    "\n\n--------------------------Experiment setup----------------------------")
                print("There will be ", len(
                    tokens), " tokens traded in the dex-aggregator trading data set")
                print("Buffer is only allowed in ", len(
                    buffer_allow_listed_tokens), "tokens")
                print("Total buffer investment", initial_buffer_value_in_usd,
                      "[USD] and in each token there is a buffer of:", initial_buffer_value_in_usd/len(buffer_allow_listed_tokens), "[USD]")

            result_df = compute_buffer_evolution(
                df, buffers, token_prices, buffer_allow_listed_tokens)
            ratio_internal_trades = round(result_df['nr_of_internal_trades'].sum(
            )/(result_df['nr_of_internal_trades'].sum()+result_df['nr_of_external_trades'].sum()), 4)

            # result array corresponds to the header defined above
            internally_traded_volume = round(result_df['internally_matched_vol_across_time'].sum(
            ))
            externally_traded_volume = round(result_df['external_vol_across_time'].sum(
            ))
            result_array = [initial_buffer_value_in_usd,
                            len(buffer_allow_listed_tokens),
                            ratio_internal_trades,
                            round(internally_traded_volume /
                                  (externally_traded_volume + internally_traded_volume), 2),
                            round(revenue_scaling_factor *
                                  internally_traded_volume*0.0005, 2),
                            round((gas_cost_internal_trade*ratio_internal_trades + (gas_cost_over_head_best_router +
                                                                                    gas_cost_avg_amm_trade)*(1-ratio_internal_trades))/gas_cost_avg_amm_trade, 4),
                            ]
            if verbose_logging:
                print(
                    "\n-------------------------------Result--------------------------------")
                print("Ratio of internal trades in comparison to total trades",
                      ratio_internal_trades)
                print("Total number of internal and external trades considered",
                      (result_df['nr_of_internal_trades'].sum()+result_df['nr_of_external_trades'].sum()))

                print("Total internally traded volume",
                      internally_traded_volume, "and total fees from avoid amm ", internally_traded_volume*0.0005)
                print("Percentage of internally matched volume",
                      internally_traded_volume / (externally_traded_volume + internally_traded_volume))
                print(
                    "\n-------------------------------Compressed result--------------------------------")
                print(header)
                print(result_array)

            with open(result_file, 'a', encoding='UTF8') as f:
                writer = csv.writer(f)
                writer.writerow(result_array)
