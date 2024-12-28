import re


def simplify_filter(filter_str, benchmark):
    # Custom transformations for specific cases
    if benchmark == 'tpch':
        print("Original Filter:", filter_str)  # Debug print
        # Remove unnecessary escape characters
        filter_str = re.sub(r'\\\\', '', filter_str)
        print("After removing escape characters:", filter_str)  # Debug print

        # Split the filter string at 'AND' and 'OR' to handle each condition separately
        conditions = re.split(r' AND | OR ', filter_str)
        transformed_conditions = []

        for condition in conditions:
            # Remove type casts
            condition = re.sub(r'::[a-zA-Z\s]+', '', condition)
            print("After removing type casts:", condition)  # Debug print

            # Custom transformations for specific cases
            # Transform (p_type LIKE '%TIN') to (p_type,'TIN')
            condition = re.sub(
                r'p_type LIKE \'%([^%]+)\'', r'(p_type,\'\1\')', condition)
            # Transform (p_size = 1) to (p_size,1)
            condition = re.sub(r'p_size = (\d+)', r'(p_size,\1)', condition)
            # Transform (l_shipdate > '1995-03-27') to (l_shipdate ,'1995-03-27')
            condition = re.sub(
                r'l_shipdate > \'([^\']+)\'', r'(l_shipdate,\'\1\')', condition)
            # Transform (c_mktsegment = 'HOUSEHOLD') to (c_mktsegment,'HOUSEHOLD')
            condition = re.sub(
                r'c_mktsegment = \'([^\']+)\'', r'(c_mktsegment,\'\1\')', condition)
            # Transform (r_name = 'AFRICA') to (r_name,'AFRICA')
            condition = re.sub(
                r'r_name = \'([^\']+)\'', r'(r_name,\'\1\')', condition)
            # Transform (o_orderdate >= '1993-01-01') to (o_orderdate,'1993-01-01')
            condition = re.sub(
                r'o_orderdate >= \'([^\']+)\'', r'(o_orderdate,\'\1\')', condition)
            # Transform (o_orderdate <= '1996-12-31') to (o_orderdate,'1996-12-31')
            condition = re.sub(
                r'o_orderdate <= \'([^\']+)\'', r'(o_orderdate,\'\1\')', condition)
            # Transform (o_orderdate < '1994-01-01 00:00:00') to (o_orderdate,'1994-01-01')
            condition = re.sub(
                r'o_orderdate < \'([^\']+)\s[^\']+\'', r'(o_orderdate,\'\1\')', condition)
            # Transform (l_shipdate >= '1995-01-01') to (l_shipdate,'1995-01-01')
            condition = re.sub(
                r'l_shipdate >= \'([^\']+)\'', r'(l_shipdate,\'\1\')', condition)
            # Transform (l_shipdate <= '1996-12-31') to (l_shipdate,'1996-12-31')
            condition = re.sub(
                r'l_shipdate <= \'([^\']+)\'', r'(l_shipdate,\'\1\')', condition)
            # Transform (l_shipdate < '1993-02-01 00:00:00') to (l_shipdate,'1993-02-01')
            condition = re.sub(
                r'l_shipdate < \'([^\']+)\s[^\']+\'', r'(l_shipdate,\'\1\')', condition)
            # Transform (n_name = 'ALGERIA') to (n_name,'ALGERIA')
            condition = re.sub(
                r'n_name = \'([^\']+)\'', r'(n_name,\'\1\')', condition)
            # Transform (p_type = 'TIN') to (p_type,'TIN')
            condition = re.sub(
                r'p_type = \'([^\']+)\'', r'(p_type,\'\1\')', condition)
            # Transform (p_brand = 'BRAND#11') to (p_brand,'BRAND#11')
            condition = re.sub(
                r'p_brand = \'([^\']+)\'', r'(p_brand,\'\1\')', condition)
            # Transform (p_container = 'SM CASE') to (p_container,'SM CASE')
            condition = re.sub(
                r'p_container = \'([^\']+)\'', r'(p_container,\'\1\')', condition)

            transformed_conditions.append(condition)

        # Recombine the transformed conditions with commas
        filter_str = ','.join(transformed_conditions)

    return filter_str


# Test cases
test_filters = [
    "(p_type LIKE '%TIN' AND p_size = 1)",
    "(l_shipdate > '1995-03-27'::date)",
    "(c_mktsegment = 'HOUSEHOLD'::bpchar)",
    "(r_name = 'AFRICA'::bpchar)",
    "((o_orderdate >= '1993-01-01'::date) AND (o_orderdate <= '1996-12-31'::date))",
    "((l_shipdate >= '1995-01-01'::date) AND (l_shipdate <= '1996-12-31'::date))",
    "((n_name = 'ALGERIA'::bpchar) OR (n_name = 'ARGENTINA'::bpchar))",
    "p_type = 'TIN'",
    "((o_orderdate >= '1995-01-01'::date) AND (o_orderdate <= '1996-12-31'::date))",
    "((l_shipdate >= '1993-01-01'::date) AND (l_shipdate < '1993-02-01 00:00:00'::timestamp without time zone))",
    "(((p_type): : text ~~ '%TIN': : text) AND (p_size = 1))"
]

benchmark = 'tpch'

for test_filter in test_filters:
    print("Testing filter:", test_filter)
    simplified_filter = simplify_filter(test_filter, benchmark)
    print("Simplified Filter:", simplified_filter)
    print("-" * 50)
