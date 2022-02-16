-- This is formatted according to what buffer_research2.py is expecting
Select usd_amount, block_number as block, token_a_address as sell_token, token_b_address as buy_token, project, token_a_amount as sell_amount, token_b_amount as buy_amount
from dex.trades d
inner join ethereum."transactions" t on d.tx_hash = t.hash
where category = 'Aggregator' and d.block_time > '2022-02-01 00:00' and d.block_time < '2022-02-08 00:00'
and project = 'Gnosis Protocol'
and project != '0x API' -- exluding 0x API because of weird double counting
and project != 'Matcha' -- exluding matcha because of weird double counting