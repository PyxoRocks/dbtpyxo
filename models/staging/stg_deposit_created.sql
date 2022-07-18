select customer_id,
organization_id,
party_id,
temps,
json_extract_path_text ("event", 'transactionId') as transactionId,
json_extract_path_text ("event", 'depositId') as depositId,
json_extract_path_text ("event", 'location') as location,
json_extract_path_text ("event", 'expiresAt') as expiresAt,
json_extract_path_text ("event", 'initialAmount') as initialAmount
from {{ref('raw_bridge')}}
where type='DepositCreated'
order by temps desc