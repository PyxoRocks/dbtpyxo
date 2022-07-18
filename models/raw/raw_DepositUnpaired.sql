select
customer_id,
organization_id,
party_id,
time as temps,
event,
type,
source,
json_extract_path_text ("event", 'depositId') as depositId,
json_extract_path_text ("event", 'pairedProducts') as pairedProducts,
json_extract_path_text ("event", 'returnedProducts') as returnedProducts
from {{ source('prod', 'raw_bridge_events') }}
where (type='DepositUnpaired' and source='deposit-events')
order by time desc