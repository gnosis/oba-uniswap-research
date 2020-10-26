import csv
import ast


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
            (block_number, index, gas_price, sell_amount,
             buy_amount, path, address, output_amounts) = row

            path = ast.literal_eval(path)
            path = ['0x' + address for address in path]
            entry = orders[block_number] if block_number in \
                orders else list()
            if read_swaps_splitted:
                for i in range(0, len(path)-1):
                    entry.append(
                        {"sellToken": path[i],
                         'buyToken': path[i+1]})
            else:
                entry.append(
                    {"sellToken": path[0],
                     'buyToken': path[-1]})
            orders[block_number] = entry
        return orders
