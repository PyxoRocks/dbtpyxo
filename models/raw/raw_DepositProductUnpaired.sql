select
customer_id,
organization_id,
party_id,
time as temps,
event,
type,
source,
json_extract_path_text ("event", 'depositId') as depositId,
json_extract_path_text ("event", 'product') as productP,
json_extract_path_text ("event", 'unpairedAt') as unpairedAt
from {{ source('prod', 'raw_bridge_events') }}
where type='DepositProductUnpaired'
order by time desc