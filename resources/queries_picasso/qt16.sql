select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_retailprice = '{RETAIL_PRICE}'
    and ps_suppkey in (
        select
            s_suppkey
        from
            supplier
        where
            s_acctbal = '{S_ACCTBAL}'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;