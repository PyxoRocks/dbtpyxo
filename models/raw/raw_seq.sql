with seq_0_9 as (
    select 0 as num
    union all select 1 as num
    union all select 2 as num
    union all select 3 as num
    union all select 4 as num
    union all select 5 as num
    union all select 6 as num
    union all select 7 as num
    union all select 8 as num
    union all select 9 as num
 ),

seq_0_999 as (
    select a.num + b.num * 10 + c.num * 100 as num
    from seq_0_9 a, seq_0_9 b, seq_0_9 c
 )
select * from seq_0_999
order by num