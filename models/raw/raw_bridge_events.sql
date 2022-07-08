select * from
{{ source('prod', 'raw_bridge_events') }}
order by time desc