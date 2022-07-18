WITH exploded_array AS (
select customer_id,
organization_id,
party_id,
temps,
transactionId,
depositId,
location,
json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(pairedProducts, seq.i) , 'type' ) AS productTypeP,
json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(pairedProducts, seq.i) , 'id' ) AS productIdP
from {{ref('raw_deposit_paired')}}, {{ ref('raw_seq') }} AS seq
WHERE (seq.i < JSON_ARRAY_LENGTH(pairedProducts))
order by temps desc
)
SELECT *
FROM exploded_array
