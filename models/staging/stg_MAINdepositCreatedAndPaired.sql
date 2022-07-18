select 
distinct dcp.depositId as depositId,
dc.customer_id as customer_id,
dcp.organization_id as organization_id,
dcp.party_id as party_id,
dcp.location as location,
dcp.temps as temps,
dcp.pairingMode as pairingMode,
dcp.expiresAt as expiresAt,
dcp.createdAt as createdAt,
JSON_ARRAY_LENGTH(dcp.pairedProducts) as NbrOfPairedProducts,
dcp.totalAmount as totalAmount
FROM {{ ref('stg_depositCreatedAndPaired') }} as dcp left join {{ ref('stg_deposit_created') }} as dc
on dcp.depositId=dc.depositId
--group by dcp.depositId, dc.customer_id, dcp.organization_id, dcp.party_id, dcp.location, dcp.temps, dcp.pairingMode, dcp.expiresAt, dcp.createdAt, dcp.totalAmount
order by temps desc