import csv
import ast
from random import sample


def read_swaps_from_csv(filename, read_swaps_splitted=False, data_usage_percentage=50):
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        orders = dict()
        data = sample(data[1:], len(data) * data_usage_percentage // 100)
        for cnt, row in enumerate(data):
            (block_number, index, gas_price, sell_amount,
             buy_amount, path, address, output_amounts) = row
            output_amounts = ast.literal_eval(output_amounts.replace('L', ''))
            path = ast.literal_eval(path)
            path = ['0x' + address for address in path]
            block_number = int(block_number)
            entry = orders[block_number] if block_number in \
                orders else list()
            if read_swaps_splitted:
                for count, (sell_token, buy_token) in enumerate(zip(path[:-1], path[1:])):
                    entry.append(
                        {"sellToken": sell_token,
                         "buyToken": buy_token,
                         "address": address,
                         "sellAmount": output_amounts[count],
                         "buyAmount": output_amounts[count+1]})
            else:
                entry.append(
                    {"sellToken": path[0],
                     'buyToken': path[-1],
                     'address': address,
                     "sellAmount": output_amounts[0],
                     "buyAmount": output_amounts[-1]})
            orders[block_number] = entry
        return orders
