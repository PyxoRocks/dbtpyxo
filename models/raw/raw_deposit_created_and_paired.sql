with deposit_created_and_paired as(
    SELECT customer_id,
    organization_id,
    party_id,
    json_extract_path_text ("event", 'location') as location,
    time as time,
    json_extract_path_text ("event", 'depositId') as deposit_id,
    json_extract_path_text ("event", 'pairingMode') as pairing_mode,
    json_extract_path_text ("event", 'expiresAt') as expires_at,
    json_extract_path_text ("event", 'createdAt') as created_at,
    json_extract_path_text ("event", 'totalAmount') as total_amount,
    json_extract_path_text ("event", 'pairedProducts') as paired_products
    FROM {{ ref('raw_bridge') }}
    where type='DepositCreatedAndPaired'
    order by time desc
),

exploded_array as(
    SELECT 
      customer_id,
      organization_id,
      party_id,
      location,
      time as time,
      deposit_id,
      pairing_mode,
      expires_at,
      created_at,
      total_amount,
      paired_products,
      json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(paired_products, seq.num) , 'type' ) AS product_type,
      json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(paired_products, seq.num) , 'id' ) AS product_id
      --json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i) , 'products_id' ) AS name
    FROM deposit_created_and_paired, {{ ref('raw_seq') }} AS seq
    WHERE (seq.num < JSON_ARRAY_LENGTH(paired_products))
    order by time desc
)
  
SELECT *
FROM exploded_array