Select 
p.contract_address as token, price * 10^(18-decimals) as usd_price, decimals
from prices.usd p inner join ethereum."blocks" ett on ett.time = p.minute 
where minute > {{start_time}} and minute <{{end_time}}
