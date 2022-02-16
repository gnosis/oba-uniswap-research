from src.oba_from_uniswap.rebalance import compute_buffers_constant
import pickle
from turtle import update

from src.dune_api.dune_analytics import DuneAnalytics
import pandas as pd
from math import log
import csv
from dotenv import load_dotenv
from copy import deepcopy

load_dotenv()


def prepare_datafile(df):
    df['batch_start_time'] = df['block']
    # this is just to allow running the code in rebalance.py (which has column name
    # hardcoded to eth for no reason - can be any currency).
    df['max_vol_eth'] = df.usd_amount
    return df


def get_token_prices_in_usd(df):
    """Computes a dictionary containing prices for all tokens in df at each block."""
    token_prices = {}

    # get prices of buy and sell token
    def update_prices(r):
        if r.block not in token_prices.keys():
            token_prices[r.block] = {}
        token_prices[r.block][r.sell_token] = r.usd_amount/r.sell_amount
        token_prices[r.block][r.buy_token] = r.usd_amount/r.buy_amount
    df.apply(axis=1, func=update_prices)

    # get init price from first trade in the future trading that token
    init_price = {}
    for prices_at_block in token_prices.values():
        for t, p in prices_at_block.items():
            if t not in init_price.keys():
                init_price[t] = p
    token_prices[list(token_prices.keys())[0] - 1] = deepcopy(init_price)

    # update token prices to include prices for all tokens (not just the traded 2)
    for block, prices_at_block in token_prices.items():
        for t, p in prices_at_block.items():
            init_price[t] = p
        token_prices[block] = deepcopy(init_price)

    return token_prices


def compute_rebalanced_volume(df, init_buffer_size, token_prices_in_usd):
    tokens = pd.concat([df.sell_token, df.buy_token]).unique()
    buffers = {t: init_buffer_size/len(tokens) for t in tokens}
    buffers_across_time = compute_buffers_constant(
        df, buffers, token_prices_in_usd)
    vol_across_time = buffers_across_time.loc[:,
                                              buffers_across_time.columns != 'rebalanced_vol'].sum(axis=1)
    final_vol = vol_across_time.iloc[-1]
    return buffers_across_time.rebalanced_vol.sum(), final_vol


def compute_rebalances_table(df, token_prices_in_usd):
    r = []
    for initial_buffer_vol in [2_000_000, 10_000_000, 50_000_000]:
        rebalanced_vol, final_vol = \
            compute_rebalanced_volume(
                df, initial_buffer_vol, token_prices_in_usd)
        r.append({
            'initial_buffer_vol': initial_buffer_vol,
            'final_buffer_vol': final_vol,
            'rebalanced_vol': rebalanced_vol,
            'impermanent_loss': initial_buffer_vol - final_vol
        })
    r_df = pd.DataFrame.from_records(r)
    r_df['total_vol'] = df.usd_amount.sum()
    r_df['rebalanced_frac'] = r_df.rebalanced_vol / r_df.total_vol
    return r_df


if __name__ == '__main__':

    fetch_data_from_dune = True
    verbose_logging = False

    if fetch_data_from_dune:
        dune_connection = DuneAnalytics.new_from_environment()
        dune_data = dune_connection.fetch(
            # NOTE: Check that this query wasn't changed!
            query_filepath="./src/dune_api/queries/gnosis_protocol_trades.sql",
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
    df = df[(df['usd_amount'].notna() & df['sell_amount'].notna()
             & df['buy_amount'].notna())]

    df = prepare_datafile(df)
    token_prices_in_usd = get_token_prices_in_usd(df)

    df = compute_rebalances_table(df, token_prices_in_usd)

    print(df)
