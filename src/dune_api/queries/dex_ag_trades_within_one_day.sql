Select token_a_symbol, token_b_symbol, usd_amount, block_number, token_a_address, token_b_address, project
from dex.trades d
inner join ethereum."transactions" t on d.tx_hash = t.hash
where category = 'Aggregator' and d.block_time > '2022-02-03 00:00' and d.block_time < '2022-02-04 00:00'
and project = '1inch'
and project != '0x API' -- exluding 0x API because of weird double counting
and project != 'Matcha' -- exluding matcha because of weird double counting