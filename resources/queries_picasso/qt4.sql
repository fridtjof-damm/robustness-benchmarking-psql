select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_totalprice = '{TOTAL_PRICE}'
    and exists (
        select *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_extendedprice = '{EXTENDED_PRICE}'
    )
group by
    o_orderpriority
order by
    o_orderpriority;