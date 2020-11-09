import argparse
import json
import csv

def main(oba_file, scp_file, oba_scp_file):
    with open(oba_file, "r") as f:
        oba = json.load(f)['orders']
    with open(scp_file, "r") as f:
        reader = csv.reader(f)
        scp = [{
            'timestamp': int(row[0]),
            'sellToken': row[1].split('-')[1],
            'buyToken': row[1].split('-')[0],
            'maxSellAmount': float(row[3]),
            'limitXRate': [[
                1, row[1].split('-')[0], 
            ],[
                float(row[4]), row[1].split('-')[1]
            ]],
            'fillOrKill': False
        } for row in reader]

    # remove SCP orders after last oba order
    scp = [o for o in scp if o['timestamp'] <= oba[-1]['uniswap']['timestamp'] - 17]

    # remove all SCP orders except the those on last minute before the first oba order
    scp = [o for o in scp if o['timestamp'] >= oba[0]['uniswap']['timestamp'] - 60]

    merged = oba + scp
    merged = sorted(
        merged,
        key=lambda o: o['timestamp'] if 'timestamp' in o else o['uniswap']['timestamp']
    )

    with open(oba_scp_file, "w+") as f:
        json.dump({'orders': merged}, f, indent=2)
    
parser = argparse.ArgumentParser(
    description='Insert SCP LP orders in an oba raw instance file.')

parser.add_argument(
    'oba_file',
    type=str,
    help='Path to OBA raw instance file.'
)

parser.add_argument(
    'scp_file',
    type=str,
    help='Path to SCP csv file.'
)

parser.add_argument(
    'oba_scp_file',
    type=str,
    help='Path to merged OBA instance file.'
)

args = parser.parse_args()

main(args.oba_file, args.scp_file, args.oba_scp_file)
