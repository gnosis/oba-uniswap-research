WITH 
path_trades as (
Select DISTINCT(tx_hash)
from dex.trades d
where category = 'Aggregator' and d.block_time > '2022-02-10 00:00' and d.block_time < '2022-02-18 00:00'
and project = 'Gnosis Protocol'
)
Select  d.tx_hash, d.token_a_symbol, d.token_b_symbol, d.usd_amount, t.block_number, d.token_a_address, d.token_b_address, d.project, d.version, t.gas_used
from dex.trades d 
inner join ethereum."transactions" t on d.tx_hash = t.hash and t.block_time > '2022-02-10 00:00' and t.block_time < '2022-02-18 00:00'
inner join path_trades i on d.tx_hash = i.tx_hash
where category = 'DEX' and d.block_time > '2022-02-10 00:00' and d.block_time < '2022-02-18 00:00' 