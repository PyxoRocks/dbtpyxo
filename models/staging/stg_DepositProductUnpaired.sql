select party_id,
temps,
depositId,
unpairedAt,
json_extract_path_text ("productP", 'id') as idP,
json_extract_path_text ("productP", 'type') as typeP
from {{ ref('raw_DepositProductUnpaired') }}
order by temps desc
