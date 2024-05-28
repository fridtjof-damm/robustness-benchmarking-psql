import datetime
import itertools as it

SCALE_FACTOR = 10

def generate_query(template: str, query_id: int) -> tuple[list[str],list[tuple]]:
    queries: list[str] = []
    parameters: list[tuple] = []
    match query_id:
        case 1:
            for param1 in range(60, 121):
                queries.append(template.format(DELTA = param1))
                parameters.append((param1,))
        case 2:
            for s in range(1,51):
                for t in type_syllables_3:
                    for region in regions:
                        queries.append(template.format(SIZE = s, TYPE = t, REGION = region[1]))
                        parameters.append((s,t,region[1]))
        case 3:
            for seg in segments:
                for d3 in dates_03:
                    queries.append(template.format(SEGMENT = seg, DATE = d3))
        case 4:
            for param4 in dates_04:
                queries.append(template.format(DATE = param4))
        case 5:
            for param5 in it.product(regions, dates_05):
                queries.append(template.format(REGION = param5[0][1],DATE = param5[1]))
        case 6:
            for param6 in it.product(dates_05, discount, quantity):
                queries.append(template.format(DATE = param6[0], DISCOUNT = param6[1], QUANTITY = param6[2]))
        case 7:
            for nation_pair in it.permutations(nations, 2):
                queries.append(template.format(NATION1 = nation_pair[0][1], NATION2 = nation_pair[1][1]))
        case 8:
            for param8 in it.product(nations,regions,type_syllables_3):
                if param8[0][2] == param8[1][0]:
                    queries.append(template.format(NATION = param8[0][1], REGION = param8[1][1], TYPE = param8[2]))
        case 9:
            for param9 in colors:
                queries.append(template.format(COLOR = param9))
        case 10:
            for param10 in dates_10:
                queries.append(template.format(DATE = param10))
        case 11:
            for param11 in nations:
                queries.append(template.format(NATION = param11[1], FRACTION = 0.0001 / SCALE_FACTOR))
        case 12:
            for param12 in it.product(it.permutations(modes, 2), dates_05):
                queries.append(template.format(SHIPMODE1 = param12[0][0], SHIPMODE2 = param12[0][1], DATE = param12[1]))
        case 13:
            for param13 in it.product(word1, word2):
                queries.append(template.format(WORD1 = param13[0], WORD2 = param13[1]))
        case 14:
            for param14 in dates_05:
                queries.append(template.format(DATE = param14))
        case 15:
            for i, param15 in enumerate(dates_04):
                queries.append(template.format(DATE = param15, STREAM_ID = i + 1)) 
        case 16:
            pass
            for param16 in it.product(brand, it.product(type_syllables_1, type_syllables_2), range(8, 51)):
                queries.append(template.format(BRAND = param16[0], TYPE = f'{param16[1][0]} {param16[1][1]}', SIZE1 = 1, SIZE2 = 2, SIZE3 = 3, SIZE4 = 4, SIZE5 = 5, SIZE6 = 6, SIZE7 = 7, SIZE8 =  param16[2]))
        case 17:
            for param17 in it.product(brand, it.product(container_syllables_1, container_syllables_2)):
                queries.append(template.format(BRAND = param17[0], CONTAINER = f'{param17[1][0]} {param17[1][1]}'))
        case 18:
            for param18 in range(312,316):
                queries.append(template.format(QUANTITY = param18)) 
        case 19:
            pass
            for param19 in it.product(range(1, 11), range(10, 21), range(20, 31), it.permutations(brand, 3)):
                queries.append(template.format(QUANTITY1 = param19[0], QUANTITY2 = param19[1], QUANTITY3 = param19[2], BRAND1 = param19[3][0], BRAND2 = param19[3][1], BRAND3 = param19[3][2])) # Können die brands die gleichen sein?
        case 20:
            pass
            for param20 in it.product(colors, dates_05, nations):
                queries.append(template.format(COLOR = param20[0], DATE = param20[1], NATION = param20[2][1]))
        case 21:
            pass
            for param21 in nations:
                queries.append(template.format(NATION = param21[1]))
        case 22:
            pass
            phone_num_offset = 10
            for i in range(6,len(nations)):
               queries.append(template.format(I1 = (nations[0][0] + phone_num_offset) , I2 = (nations[1][0] + phone_num_offset), I3 = (nations[2][0] + phone_num_offset), I4 = (nations[3][0] + phone_num_offset), I5 = (nations[4][0] + phone_num_offset), I6 = (nations[2][0] + phone_num_offset), I7 = (nations[i][0]+ phone_num_offset)))          
    # print(query_id)
    #print(parameters)
    return queries,parameters

type_syllables_1 = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
type_syllables_2 = ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED']
type_syllables_3 = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']
regions = [(0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'), (3, 'EUROPE'), (4, 'MIDDLE EAST')]
segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
dates_03 = [datetime.date(1995, 3, 31) - datetime.timedelta(days=x) for x in range(31)]
dates_04 = [datetime.date(1993 + month // 12, 1 + month % 12, 1) for month in range(58)]
# 05:  DATE is the first of January of a randomly selected year within 1993..1997 
dates_05 = [datetime.date(1993 + year, 1, 1) for year in range(5)]
dates_10 = [datetime.date(1993 + month // 12, 1 + month % 12, 1) for month in range(25)][1:]
discount = [ds / 100 for ds in range(2, 10)]
quantity = [24, 25]
nations = [(0, 'ALGERIA', 0), (1, 'ARGENTINA', 1), (2, 'BRAZIL', 1), (3, 'CANADA', 1), (4, 'EGYPT', 4),
            (5, 'ETHIOPIA', 0), (6, 'FRANCE', 3), (7, 'GERMANY', 3), (8, 'INDIA', 2), (9, 'INDONESIA', 2),
            (10, 'IRAN', 4), (11, 'IRAQ', 4), (12, 'JAPAN', 2), (13, 'JORDAN', 4), (14, 'KENYA', 0),
            (15, 'MOROCCO', 0), (16, 'MOZAMBIQUE', 0), (17, 'PERU', 1), (18, 'CHINA', 2), (19, 'ROMANIA', 3),
            (20, 'SAUDI ARABIA', 4), (21, 'VIETNAM', 2), (22, 'RUSSIA', 3), (23, 'UNITED KINGDOM', 3),
            (24, 'UNITED STATES', 1)]
colors = ['almond', 'antique', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanched', 'blue', 'blush',
              'brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon', 'chocolate', 'coral', 'cornflower',
              'cornsilk', 'cream', 'cyan', 'dark', 'deep', 'dim', 'dodger', 'drab', 'firebrick', 'floral',
              'forest',
              'frosted', 'gainsboro', 'ghost', 'goldenrod', 'green', 'grey', 'honeydew', 'hot', 'indian', 'ivory',
              'khaki', 'lace', 'lavender', 'lawn', 'lemon', 'light', 'lime', 'linen', 'magenta', 'maroon',
              'medium',
              'metallic', 'midnight', 'mint', 'misty', 'moccasin', 'navajo', 'navy', 'olive', 'orange', 'orchid',
              'pale', 'papaya', 'peach', 'peru', 'pink', 'plum', 'powder', 'puff', 'purple', 'red', 'rose', 'rosy',
              'royal', 'saddle', 'salmon', 'sandy', 'seashell', 'sienna', 'sky', 'slate', 'smoke', 'snow',
              'spring',
              'steel', 'tan', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'yellow']
modes = ['REG AIR', 'AIR', 'RAIL', 'SHIP', 'TRUCK', 'MAIL', 'FOB']
word1 = ['special', 'pending', 'unusual', 'express']
word2 = ['packages', 'requests', 'accounts', 'deposits']
container_syllables_1 = ['SM', 'LG', 'MED', 'JUMBO', 'WRAP']
container_syllables_2 = ['CASE', 'BOX', 'BAG', 'JAR', 'PKG', 'PACK', 'CAN', 'DRUM']
brand = ['BRAND#' + str(i[0]) + str(i[1]) for i in it.permutations(range(1, 6), 2)]
