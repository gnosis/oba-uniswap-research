import csv
import ast


def read_swaps_from_csv(filename, read_swaps_splitted=False, assume_only_halve_is_migrating=False):
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        orders = dict()
        first = True
        for cnt, row in enumerate(data):
            # Skip header.
            if first:
                first = False
                continue
            if assume_only_halve_is_migrating and cnt % 2 == 0:
                continue
            (block_number, index, gas_price, sell_amount,
             buy_amount, path, address, output_amounts) = row

            path = ast.literal_eval(path)
            path = ['0x' + address for address in path]
            block_number = int(block_number)
            entry = orders[block_number] if block_number in \
                orders else list()
            if read_swaps_splitted:
                for sell_token, buy_token in zip(path, path[1:]):
                    entry.append(
                        {"sellToken": sell_token,
                         "buyToken": buy_token,
                         "address": address})
            else:
                entry.append(
                    {"sellToken": path[0],
                     'buyToken': path[-1],
                     'address': address})
            orders[block_number] = entry
        return orders
