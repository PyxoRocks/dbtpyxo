select json_extract_path_text ("event", 'customerId') as customerId,
organization_id,
party_id,
json_extract_path_text ("event", 'location') as location,
temps,
json_extract_path_text ("event", 'transactionId') as transactionId,
json_extract_path_text ("event", 'depositId') as depositId,
json_extract_path_text ("event", 'expiresAt') as expiresAt,
json_extract_path_text ("event", 'productId') as productId,
json_extract_path_text ("event", 'productType') as productType,
json_extract_path_text ("event", 'nbPairedProducts') as nbPairedProducts,
json_extract_path_text ("event", 'nbReturnedProducts') as nbReturnedProducts
from {{ref('raw_bridge')}}
where type='ProductUnpaired'
order by temps desc