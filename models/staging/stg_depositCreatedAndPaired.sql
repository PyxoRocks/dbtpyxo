WITH exploded_array AS (
    SELECT 
      customer_id,
      organization_id,
      party_id,
      location,
      temps,
      depositId,
      pairingMode,
      expiresAt,
      createdAt,
      totalAmount,
      pairedProducts,
      json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(pairedProducts, seq.i) , 'type' ) AS productType,
      json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(pairedProducts, seq.i) , 'id' ) AS productId
      --json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i) , 'products_id' ) AS name
    FROM {{ ref('raw_depositCreatedAndPaired') }}, {{ ref('raw_seq') }} AS seq
    WHERE (seq.i < JSON_ARRAY_LENGTH(pairedProducts))
    order by temps desc
)
  
SELECT *
FROM exploded_array