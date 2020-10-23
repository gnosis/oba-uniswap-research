import csv
import ast


def read_swaps_from_csv(filename):
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
            (block_number, index, gas_price, sell_amount,
             buy_amount, path, address, output_amounts) = row

            path = ast.literal_eval(path)
            path = ['0x' + address for address in path]
            entry = orders[block_number] if block_number in \
                orders else list()
            entry.append(
                {"sellToken": path[0],
                 'buyToken': path[-1]})
            orders[block_number] = entry
        return orders
