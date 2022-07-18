select customerId,
dcpp.organization_id,
dcpp.party_id,
dcpp.location,
rb.temps,
rb.transactionId,
rb.depositId,
rb.expiresAt,
rb.productId,
rb.productType,
rb.nbPairedProducts,
rb.nbReturnedProducts
from {{ref('raw_product_unpaired')}} as rb left join {{ref('stg_depositCreatedAndPaired')}} as dcpp
on rb.depositId=dcpp.depositId
order by temps desc