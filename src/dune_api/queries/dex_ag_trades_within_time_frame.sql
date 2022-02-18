Select token_a_symbol, token_b_symbol, usd_amount, block_number, token_a_address, token_b_address, token_a_amount_raw, token_b_amount_raw, project
from dex.trades d
inner join ethereum."transactions" t on d.tx_hash = t.hash
where category = 'Aggregator' and d.block_time > {{start_time}} and d.block_time < {{end_time}} and t.block_time > {{start_time}} and t.block_time < {{end_time}}
-- and project = 'Gnosis Protocol'
and project != 'Paraswap' -- paraswap settles around 22-24 of Dec 2020 tokens at weird prices
and project != '0x API' -- exluding 0x API because of weird double counting
and project != 'Matcha' -- exluding matcha because of weird double counting