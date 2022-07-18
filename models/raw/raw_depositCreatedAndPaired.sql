SELECT 
customer_id,
organization_id,
party_id,
json_extract_path_text ("event", 'location') as location,
temps,
json_extract_path_text ("event", 'depositId') as depositId,
json_extract_path_text ("event", 'pairingMode') as pairingMode,
json_extract_path_text ("event", 'expiresAt') as expiresAt,
json_extract_path_text ("event", 'createdAt') as createdAt,
json_extract_path_text ("event", 'totalAmount') as totalAmount,
json_extract_path_text ("event", 'pairedProducts') as pairedProducts
FROM {{ ref('raw_bridge') }}
where type='DepositCreatedAndPaired'
order by temps desc