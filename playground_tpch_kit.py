import duckdb
import datetime

cursor = duckdb.connect(".open tpch-kit-v.duckdb")


def generate_query(template: str, query_id: int) -> list[str]:
    queries: list[str] = []
    match query_id:
        case 1:
            for delta in range(60, 125, 5):
                queries.append(template.format(DELTA = delta))
        case 2:
            pass
            # return template.format(SIZE = , TYPE = , REGION = )
        case 3:
            for s in segments:
                for d in dates_03:
                    queries.append(template.format(SEGMENT = s, DATE = d))
        case 4:
            print(query_id
        case 5:
            print(query_id)        
        case 6:
            print(query_id)
        case 7:
            print(query_id)
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
    with open(f'queries/{query_id}.sql', encoding="UTF8") as statement_file:
        template = statement_file.read()
        queries = generate_query(template, query_id)
        for query in queries:
            print(cursor.execute(query).df())
            print("---------------------------------------")


segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
dates_03 = [datetime.date(1995, 3, 31) - datetime.timedelta(days=x) for x in range(0,31,3)]


for pquery in range(1,23):
    run_query(pquery)

