import csv


def read_swaps_from_csv():
    with open('data/swaps_data_from_router.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        orders = dict()
        for order in data:
            path_data = order[5].split("'")
            path_data = path_data[:-1]
            if(len(path_data) > 1):
                block_number = int(order[0])
                list_start = orders[block_number] if block_number in \
                    orders else list()
                list_start.append(
                    {"sellToken": path_data[1],
                     'buyToken': path_data[-1]})
                orders[block_number] = list_start
        return orders
