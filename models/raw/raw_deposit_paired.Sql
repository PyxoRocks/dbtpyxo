with deposit_paired as(
    select customer_id,
    organization_id,
    party_id,
    time as time,
    json_extract_path_text ("event", 'transactionId') as transaction_id,
    json_extract_path_text ("event", 'depositId') as deposit_id,
    json_extract_path_text ("event", 'location') as location,
    json_extract_path_text ("event", 'pairedProducts') as paired_products
    from {{ref('raw_bridge')}}
    where type='DepositPaired'
    order by time desc
),

exploded_array AS (
    select customer_id,
    organization_id,
    party_id,
    time as time,
    transaction_id,
    deposit_id,
    location,
    json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(paired_products, seq.num) , 'type' ) AS product_type_p,
    json_extract_path_text( JSON_EXTRACT_ARRAY_ELEMENT_TEXT(paired_products, seq.num) , 'id' ) AS product_id_p
    from deposit_paired, {{ ref('raw_seq') }} AS seq
    WHERE (seq.num < JSON_ARRAY_LENGTH(paired_products))
    order by time desc
)
select * from exploded_array