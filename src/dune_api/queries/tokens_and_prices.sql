Select 
number as block_number, contract_address as token, price * 10^(18-decimals) as usd_price 
from prices.usd p inner join ethereum."blocks" t on t.time = p.minute
where minute > '2022-02-03 00:00' and minute < '2022-02-04 00:00' 