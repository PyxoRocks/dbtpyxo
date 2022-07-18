select
customer_id,
organization_id,
party_id,
time as temps,
event,
type,
source
from {{ source('prod', 'raw_bridge_events') }}
--where type='DepositUnpaired'
order by time desc