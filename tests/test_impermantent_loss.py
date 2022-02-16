
import unittest
import pandas as pd
from src.impermanent_loss_analysis_buffer_trading import apply_batch_trades_on_buffer_and_account_trade_statistic

test_trades = [{'block_number': 14123254, 'project': 'Paraswap',
                'token_a_address': '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'token_a_amount_raw': 1268800000000000000, 'token_a_symbol': 'ETH', 'token_b_address': '\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39', 'token_b_amount_raw': 1878565823884, 'token_b_symbol': 'HEX', 'usd_amount': 3395.78951154391}, {'block_number': 14123256, 'project': '0x API', 'token_a_address': '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'token_a_amount_raw': 113993750000000000, 'token_a_symbol': 'ETH', 'token_b_address': '\\x9e32b13ce7f2e80a01932b42553652e053d6ed8e', 'token_b_amount_raw': 2564494508536733779, 'token_b_symbol': 'Metis', 'usd_amount': 319.291934}]

test_trades_opposite_direction = [{'block_number': 14123254, 'project': 'Paraswap',
                                   'token_a_address': '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'token_a_amount_raw': 1268800000000000000, 'token_a_symbol': 'ETH', 'token_b_address': '\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39', 'token_b_amount_raw': 1878565823884, 'token_b_symbol': 'HEX', 'usd_amount': 3395.78951154391}, {'block_number': 14123256, 'project': '0x API', 'token_a_address': '\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39', 'token_a_amount_raw': 1.2523772e+12, 'token_a_symbol': 'Hex', 'token_b_address': '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'token_b_amount_raw': 8.4586667e+17, 'token_b_symbol': 'ETH', 'usd_amount': 2919.291934}]

test_trades_same_direction = [{'block_number': 14123254, 'project': 'Paraswap',
                               'token_a_address': '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'token_a_amount_raw': 1268800000000000000, 'token_a_symbol': 'ETH', 'token_b_address': '\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39', 'token_b_amount_raw': 1878565823884, 'token_b_symbol': 'HEX', 'usd_amount': 3395.78951154391}, {'block_number': 14123256, 'project': '0x API', 'token_a_address': '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'token_a_amount_raw': 113993750000000000, 'token_a_symbol': 'Hex', 'token_b_address': '\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39', 'token_b_amount_raw': 2564494508536733779, 'token_b_symbol': 'ETH', 'usd_amount': 2919.291934}]

prices = {'\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee': (
    3000, 18), '\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39': (0.2, 8)}


class TestApplyTradesOnBufferTest(unittest.TestCase):
    def test_apply_batch_trades_if_no_token_is_in_allow_list(self):
        df = pd.DataFrame.from_records(test_trades)

        tokens = set.union({t for t in df['token_a_address']}, {
            t for t in df['token_b_address']})

        buffer_allow_listed_tokens = list({})
        buffers = {t: 0 for t in tokens}

        sent_volume_per_pair = df.groupby(
            ["token_a_address", "token_b_address"], as_index=True).agg({"token_a_amount_raw": "sum", "token_b_amount_raw":  "sum"}).to_dict('index')
        actual_updated_buffers = apply_batch_trades_on_buffer_and_account_trade_statistic(
            sent_volume_per_pair, buffers, buffer_allow_listed_tokens)

        self.assertEqual(buffers,
                         actual_updated_buffers)

    def test_apply_batch_trades_if_one_token_is_in_allow_list(self):
        initial_buffer_value_in_usd = 1000000
        df = pd.DataFrame.from_records(test_trades)

        tokens = set.union({t for t in df['token_a_address']}, {
            t for t in df['token_b_address']})

        buffer_allow_listed_tokens = list(
            {'\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39', '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'})  # only hex and eth token has buffer
        buffers = {t: t in buffer_allow_listed_tokens and (initial_buffer_value_in_usd /
                                                           len(buffer_allow_listed_tokens) * pow(10, int(prices[t][1])) / prices[t][0]) or 0 for t in tokens if t in prices}

        initial_buffers = buffers.copy()

        sent_volume_per_pair = df.groupby(
            ["token_a_address", "token_b_address"], as_index=True).agg({"token_a_amount_raw": "sum", "token_b_amount_raw":  "sum"}).to_dict('index')
        actual_updated_buffers = apply_batch_trades_on_buffer_and_account_trade_statistic(
            sent_volume_per_pair, buffers, buffer_allow_listed_tokens)

        expected_updated_buffers = initial_buffers
        print(initial_buffers)
        expected_updated_buffers['\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39'] -= df['token_b_amount_raw'][0]
        expected_updated_buffers['\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'] += df['token_a_amount_raw'][0]
        print(actual_updated_buffers)
        self.assertEqual(actual_updated_buffers,
                         expected_updated_buffers)

    def test_apply_batch_trades_if_cow_left_overs_are_settled_internally(self):

        initial_buffer_value_in_usd = 100000
        trade_activity_threshold_for_buffers_to_be_funded = 0
        df = pd.DataFrame.from_records(test_trades_opposite_direction)

        tokens = set.union({t for t in df['token_a_address']}, {
            t for t in df['token_b_address']})

        value_counts = df['token_b_address'].value_counts(normalize=True)
        buffer_allow_listed_tokens = list({t for t in tokens if (t in value_counts
                                                                 and value_counts[t] > trade_activity_threshold_for_buffers_to_be_funded)})

        buffers = {t: t in buffer_allow_listed_tokens and (initial_buffer_value_in_usd /
                                                           len(buffer_allow_listed_tokens) * pow(10, int(prices[t][1])) / prices[t][0]) or 0 for t in tokens if t in prices}
        initial_buffers = buffers.copy()

        sent_volume_per_pair = df.groupby(
            ["token_a_address", "token_b_address"], as_index=True).agg({"token_a_amount_raw": "sum", "token_b_amount_raw":  "sum"}).to_dict('index')
        actual_updated_buffers = apply_batch_trades_on_buffer_and_account_trade_statistic(
            sent_volume_per_pair, buffers, buffer_allow_listed_tokens)

        expected_updated_buffers = initial_buffers
        expected_updated_buffers['\\x2b591e99afe9f32eaa6214f7b7629768c40eeb39'] -= df['token_b_amount_raw'][0] - \
            df['token_a_amount_raw'][1]
        expected_updated_buffers['\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'] += df['token_a_amount_raw'][0] - \
            df['token_b_amount_raw'][1]

        self.assertEqual(actual_updated_buffers,
                         expected_updated_buffers)

    def test_apply_batch_trades_if_cow_left_overs_are_settled_externally(self):

        df = pd.DataFrame.from_records(test_trades_opposite_direction)

        tokens = set.union({t for t in df['token_a_address']}, {
            t for t in df['token_b_address']})

        buffer_allow_listed_tokens = list({})
        buffers = {t: 0 for t in tokens}
        initial_buffers = buffers.copy()

        sent_volume_per_pair = df.groupby(
            ["token_a_address", "token_b_address"], as_index=True).agg({"token_a_amount_raw": "sum", "token_b_amount_raw":  "sum"}).to_dict('index')
        actual_updated_buffers = apply_batch_trades_on_buffer_and_account_trade_statistic(
            sent_volume_per_pair, buffers, buffer_allow_listed_tokens)

        expected_updated_buffers = initial_buffers

        self.assertEqual(actual_updated_buffers,
                         expected_updated_buffers)
