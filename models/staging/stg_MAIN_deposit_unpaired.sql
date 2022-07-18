select rdu.party_id,
smdcp.organization_id,
smdcp.location,
smdcp.customer_id,
rdu.temps,
smdcp.createdAt,
smdcp.expiresAt,
rdu.depositId,
JSON_ARRAY_LENGTH(pairedProducts) as nbr_of_pairedProducts,
JSON_ARRAY_LENGTH(returnedProducts) as nbr_of_returnedProducts
from {{ref('raw_DepositUnpaired')}} as rdu left join {{ref('stg_MAINdepositCreatedAndPaired')}} as smdcp
on rdu.depositId=smdcp.depositId
order by temps desc

