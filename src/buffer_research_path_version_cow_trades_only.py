import pickle
from src.buffer_research_path_version import compute_buffer_evolution

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
import csv


if __name__ == '__main__':
    fetch_data_from_dune = True

    if fetch_data_from_dune:
        dune_connection = DuneAnalytics.new_from_environment()
        dune_data = dune_connection.fetch(
            query_filepath="./src/dune_api/queries/dex_ag_path_trades_cow_trades_only.sql",
            network='mainnet',
            name="dex ag trades",
            parameters=[]
        )
        with open("data/dune_trading_data_download_cow_only.txt", "bw+") as f:
            pickle.dump(dune_data, f, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open("data/dune_trading_data_download_cow_only.txt", "rb") as f:
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
              'Ratio Internal Interactions vs Total # Interactions',
              'Ratio Internally Matched Vol',
              'Fee Revenue [USD/Year]',
              'Ratio of saved gas costs compared to non-buffer trading',
              ]
    with open(result_file, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
    # gas costs rough estimations
    # estimation as univ2 savings is around 90k and univ3 around 125k
    gas_cost_avg_amm_interaction = 100000
    gas_cost_over_head_cowswap = 90000
    gas_cost_over_head_cowswap_in_potential_v2 = 55000
    gas_cost_internal_trade_in_potential_v2 = 105000
    total_gas_consumption = df.copy()[['tx_hash', 'gas_used']
                                      ].drop_duplicates()['gas_used'].sum()

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
                # let's simulate in this case our current situation: buffer is distributed over 1500 tokens
                buffers = {t: initial_buffer_value_in_usd /
                           1500 if t in buffer_allow_listed_tokens else 0 for t in tokens}
            else:
                buffers = {t: initial_buffer_value_in_usd /
                           len(buffer_allow_listed_tokens) if t in buffer_allow_listed_tokens else 0 for t in tokens}

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
                            round((total_gas_consumption - saved_internal_trades *
                                  gas_cost_avg_amm_interaction)/total_gas_consumption, 2)]

            with open(result_file, 'a', encoding='UTF8') as f:
                writer = csv.writer(f)
                writer.writerow(result_array)
