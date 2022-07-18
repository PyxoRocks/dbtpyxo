select
dp.customer_id,
dcp.organization_id,
dcp.party_id,
dcp.location,
dcp.temps,
dcp.depositId,
dcp.pairingMode,
dcp.expiresAt,
dcp.createdAt,
dcp.totalAmount,
dcp.productType,
dcp.productId
from {{ ref('stg_depositCreatedAndPaired') }} as dcp inner join {{ ref('stg_MAINdepositCreatedAndPaired') }} as dp
on dcp.depositId=dp.depositId
order by temps desc

