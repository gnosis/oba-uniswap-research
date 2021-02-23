import random
from datetime import datetime
import ast
import csv


def read_swaps_from_csv(filename, read_swaps_splitted=False):
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        orders = dict()
        first = True
        for row in data:
            # Skip header.
            if first:
                first = False
                continue

            # ALEX format:
            (block_number, index, gas_price, sell_amount,
             buy_amount, path, address, output_amounts) = row


# COW format:
# (block_number, index, sell_amount,buy_amount, path, output_amounts, block_time, address) = row
            path = ast.literal_eval(path)
            path = ['0x' + address for address in path]
            output_amounts = ast.literal_eval(output_amounts.replace("L", ""))
            block_number = int(block_number)
            entry = orders[block_number] if block_number in \
                orders else list()
            if read_swaps_splitted:
                for sell_token, buy_token in zip(path, path[1:]):
                    entry.append(
                        {"sellToken": sell_token,
                         "buyToken": buy_token,
                         "amounts": output_amounts,
                         "address": address,
                         "block": block_number})
            else:
                entry.append(
                    {"sellToken": path[0],
                     'buyToken': path[-1],
                     'address': address,
                     "amounts": output_amounts,
                     "block": block_number})
            orders[block_number] = entry
        return orders


WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# read orders
filename = "./data/dune_download/swaps_data_from_router_11790000-11791000.csv"   # alex file
# filename="/home/marco/projects/gnosis/experimental/oba_uniswap/data/oba_from_uniswap/swaps_data_from_router_11092424-11093867.csv"   # cow file
block_orders = read_swaps_from_csv(filename, False)
# restrict to WETH - USDC pair
orders = [o for orders in block_orders.values() for o in orders if {o['sellToken'], o['buyToken']} == {
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'}]

# ​  # sample x% of orders
# # orders=random.sample(orders, round(len(orders)*0.1))
# ​  # Price of ETH (20-10-2020)
# # price_ETH=373
# # Price of ETH (18-2-2021)
price_ETH = 1573

total_amount_WETH = (sum(int(o['amounts'][0]) for o in orders if o['sellToken'] == WETH) +
                     sum(int(o['amounts'][1]) for o in orders if o['buyToken'] == WETH))*1e-18
total_vol = total_amount_WETH * price_ETH
# Group into batches
last_batch = None
batch = []
batches = []
for o in orders:
    if len(batch) > 0 and o['block']//4 > last_batch:
        batches.append(batch)
        batch = []
    batch.append(o)
    last_batch = o['block']//4

# ​  # Compute surplus
surplus = 0
for batch in batches:
    amount_weth_usdc = sum(o['amounts'][0]
                           for o in batch if o['sellToken'] == WETH) * 1e-18
    amount_usdc_eth = sum(o['amounts'][-1]
                          for o in batch if o['buyToken'] == WETH) * 1e-18
    vol_weth_usdc = amount_weth_usdc * price_ETH
    vol_usdc_weth = amount_usdc_eth * price_ETH
    matched_vol = min(vol_weth_usdc, vol_usdc_weth)
    surplus += 0.003 * matched_vol * 2

print("absolute surplus:\t", surplus)
print("absolute surplus per month:\t", surplus/len(batches) * 60*24*30)
print("relative surplus:\t", surplus/total_vol * 100, "%")
