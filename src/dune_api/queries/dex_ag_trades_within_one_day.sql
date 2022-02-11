Select token_a_symbol, token_b_symbol,token_a_amount_raw, token_b_amount_raw,usd_amount, block_number, token_a_address,token_b_address, project
from 
dex.trades d
inner join ethereum."transactions" t on d.tx_hash = t.hash
where category = 'Aggregator' and d.block_time > '2022-02-01 00:00' and d.block_time < '2022-02-05 00:00'
and project = 'Gnosis Protocol'
