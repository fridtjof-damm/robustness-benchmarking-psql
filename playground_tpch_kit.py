import datetime
import itertools as it
import duckdb

cursor = duckdb.connect(".open tpch-kit-v.duckdb")

def generate_query(template: str, query_id: int) -> list[str]:
    queries: list[str] = []
    match query_id:
        case 1:
            for delta in range(60, 125):
                queries.append(template.format(DELTA = delta))
        case 2:
            for s in range(1,51):
                for t in type_syllables_3:
                    for region in regions:
                        queries.append(template.format(SIZE = s, TYPE = t, REGION = region[1]))
        case 3:
            for s in segments:
                for d3 in dates_03:
                    queries.append(template.format(SEGMENT = s, DATE = d3))
        case 4:
            for d4 in dates_04:
                queries.append(template.format(DATE = d4))
        case 5:
            for param5 in it.product(regions, dates_05):
                queries.append(template.format(REGION = param5[0][1],DATE = param5[1]))        
        case 6:
            for var6 in it.product(dates_05, discount, quantity):
                queries.append(template.format(DATE = var6[0], DISCOUNT = var6[1], QUANTITY = var6[2]))
        case 7:
            for nation_pair in it.permutations(nations, 2):
                queries.append(template.format(NATION1 = nation_pair[0][1], NATION2 = nation_pair[1][1]))
        case 8:
            print(query_id)
        case 9:
            print(query_id)
        case 10:
            print(query_id)
        case 11:
            print(query_id)
        case 12:
            print(query_id)
        case 13:
            print(query_id)
        case 14:
            print(query_id)
        case 15:
            print(query_id)
        case 16:
            print(query_id)
        case 17:
            print(query_id)
        case 18:
            print(query_id)
        case 19:
            print(query_id)
        case 20:
            print(query_id)
        case 21:
            print(query_id)
        case 22:
            print(query_id)
    return queries
      
def run_query(query_id: int):
    # query_id = 6
    with open(f'queries/{query_id}.sql', encoding="UTF8") as statement_file:
        template = statement_file.read()
        queries = generate_query(template, query_id)
        for query in queries:
            cursor.execute(query).fetchall()
            # print("---------------------------------------")

type_syllables_3 = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']
regions = [(0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'), (3, 'EUROPE'), (4, 'MIDDLE EAST')]
segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
dates_03 = [datetime.date(1995, 3, 31) - datetime.timedelta(days=x) for x in range(31)]
dates_04 = [datetime.date(1993 + month // 12, 1 + month % 12, 1) for month in range(58)]
dates_05 = [datetime.date(1993 + year, 1, 1) for year in range(5)]
discount = [ds / 100 for ds in range(2, 10)]
quantity = [24, 25]
nations = [(0, 'ALGERIA', 0), (1, 'ARGENTINA', 1), (2, 'BRAZIL', 1), (3, 'CANADA', 1), (4, 'EGYPT', 4),
            (5, 'ETHIOPIA', 0), (6, 'FRANCE', 3), (7, 'GERMANY', 3), (8, 'INDIA', 2), (9, 'INDONESIA', 2),
            (10, 'IRAN', 4), (11, 'IRAQ', 4), (12, 'JAPAN', 2), (13, 'JORDAN', 4), (14, 'KENYA', 0),
            (15, 'MOROCCO', 0), (16, 'MOZAMBIQUE', 0), (17, 'PERU', 1), (18, 'CHINA', 2), (19, 'ROMANIA', 3),
            (20, 'SAUDI ARABIA', 4), (21, 'VIETNAM', 2), (22, 'RUSSIA', 3), (23, 'UNITED KINGDOM', 3),
            (24, 'UNITED STATES', 1)]

for pquery in range(1,23):
    run_query(pquery)
