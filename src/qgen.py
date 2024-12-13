# based on https://github.com/klauck/tpch_query_generator
from datetime import datetime, timedelta
import datetime
import itertools as it
# set scale factor used for tpc-h data
SCALE_FACTOR = 10


def generate_query(template: str, query_id: int) -> tuple[list[str], list[tuple]]:
    queries: list[str] = []
    parameters: list[tuple] = []
    match query_id:
        case 1:
            for param1 in range(60, 121):
                queries.append(template.format(DELTA=param1))
                parameters.append((param1,))
        case 2:
            for s in range(1, 51):
                for t in type_syllables_3:
                    for region in regions:
                        queries.append(template.format(
                            SIZE=s, TYPE=t, REGION=region[1]))
                        parameters.append((s, t, region[1]))
        case 3:
            for seg in segments:
                for d3 in dates_03:
                    queries.append(template.format(SEGMENT=seg, DATE=d3))
                    parameters.append((seg, str(d3)))
        case 4:
            for param4 in dates_04:
                queries.append(template.format(DATE=param4))
                parameters.append((str(param4),))
        case 5:
            for param5 in it.product(regions, dates_05):
                queries.append(template.format(
                    REGION=param5[0][1], DATE=param5[1]))
                parameters.append((param5))
        case 6:
            for param6 in it.product(dates_05, discount, quantity):
                queries.append(template.format(
                    DATE=param6[0], DISCOUNT=param6[1], QUANTITY=param6[2]))
                parameters.append((param6))
        case 7:
            for nation_pair in it.permutations(nations, 2):
                queries.append(template.format(
                    NATION1=nation_pair[0][1], NATION2=nation_pair[1][1]))
                parameters.append((nation_pair))
        case 8:
            for param8 in it.product(nations, regions, type_syllables_3):
                if param8[0][2] == param8[1][0]:
                    queries.append(template.format(
                        NATION=param8[0][1], REGION=param8[1][1], TYPE=param8[2]))
                    parameters.append((param8))
        case 9:
            for param9 in colors:
                queries.append(template.format(COLOR=param9))
                parameters.append((param9,))
        case 10:
            for param10 in dates_10:
                queries.append(template.format(DATE=param10))
                parameters.append(((param10),))
        case 11:
            for param11 in nations:
                queries.append(template.format(
                    NATION=param11[1], FRACTION=0.0001 / SCALE_FACTOR))
                parameters.append((param11))
        case 12:
            for param12 in it.product(it.permutations(modes, 2), dates_05):
                queries.append(template.format(
                    SHIPMODE1=param12[0][0], SHIPMODE2=param12[0][1], DATE=param12[1]))
                parameters.append((param12))
        case 13:
            for param13 in it.product(word1, word2):
                queries.append(template.format(
                    WORD1=param13[0], WORD2=param13[1]))
                parameters.append((param13))
        case 14:
            for param14 in dates_05:
                queries.append(template.format(DATE=param14))
                parameters.append((param14,))
        case 15:
            for i, param15 in enumerate(dates_04):
                queries.append(template.format(DATE=param15, STREAM_ID=i + 1))
                parameters.append((param15, i+1))
        case 16:
            for param16 in it.product(brand, it.product(type_syllables_1, type_syllables_2), range(8, 51)):
                queries.append(template.format(
                    BRAND=param16[0], TYPE=f'{param16[1][0]} {param16[1][1]}', SIZE1=1, SIZE2=2, SIZE3=3, SIZE4=4, SIZE5=5, SIZE6=6, SIZE7=7, SIZE8=param16[2]))
                parameters.append((param16,))
        case 17:
            for param17 in it.product(brand, it.product(container_syllables_1, container_syllables_2)):
                queries.append(template.format(
                    BRAND=param17[0], CONTAINER=f'{param17[1][0]} {param17[1][1]}'))
                parameters.append((param17,))
        case 18:
            for param18 in range(312, 316):
                queries.append(template.format(QUANTITY=param18))
                parameters.append(((param18,)))
        case 19:
            for param19 in it.product(range(1, 11), range(10, 21), range(20, 31), brand):
                queries.append(template.format(QUANTITY1=param19[0], QUANTITY2=param19[1], QUANTITY3=param19[2],
                               BRAND1=param19[3][0], BRAND2=param19[3][1], BRAND3=param19[3][2]))  # Können die brands die gleichen sein?
                parameters.append((param19))
        case 20:
            for param20 in it.product(colors, dates_05, nations):
                queries.append(template.format(
                    COLOR=param20[0], DATE=param20[1], NATION=param20[2][1]))
                parameters.append((param20))
        case 21:
            for param21 in nations:
                queries.append(template.format(NATION=param21[1]))
                parameters.append((param21,))
        case 22:
            phone_num_offset = 10
            for i in range(6, len(nations)):
                queries.append(template.format(I1=(nations[0][0] + phone_num_offset), I2=(nations[1][0] + phone_num_offset), I3=(nations[2][0] + phone_num_offset), I4=(
                    nations[3][0] + phone_num_offset), I5=(nations[4][0] + phone_num_offset), I6=(nations[i][0] + phone_num_offset), I7=(nations[i][0] + phone_num_offset)))
                parameters.append(((nations[0][0] + phone_num_offset), (nations[1][0] + phone_num_offset), (nations[2][0] + phone_num_offset), (nations[3]
                                  [0] + phone_num_offset), (nations[4][0] + phone_num_offset), (nations[i][0] + phone_num_offset), (nations[i][0] + phone_num_offset)))
    # print(query_id)
    # print(parameters)
    return queries, parameters


# tpc-h
type_syllables_1 = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
type_syllables_2 = ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED']
type_syllables_3 = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']
regions = [(0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'),
           (3, 'EUROPE'), (4, 'MIDDLE EAST')]
segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
dates_03 = [datetime.date(1995, 3, 31) - datetime.timedelta(days=x)
            for x in range(31)]
dates_04 = [datetime.date(1993 + month // 12, 1 + month % 12, 1)
            for month in range(58)]
# 05:  DATE is the first of January of a randomly selected year within 1993..1997
dates_05 = [datetime.date(1993 + year, 1, 1) for year in range(5)]
dates_10 = [datetime.date(1993 + month // 12, 1 + month % 12, 1)
            for month in range(25)][1:]
discount = [ds / 100 for ds in range(2, 10)]
quantity = [24, 25]
nations = [(0, 'ALGERIA', 0), (1, 'ARGENTINA', 1), (2, 'BRAZIL', 1), (3, 'CANADA', 1), (4, 'EGYPT', 4),
           (5, 'ETHIOPIA', 0), (6, 'FRANCE', 3), (7, 'GERMANY',
                                                  3), (8, 'INDIA', 2), (9, 'INDONESIA', 2),
           (10, 'IRAN', 4), (11, 'IRAQ', 4), (12, 'JAPAN',
                                              2), (13, 'JORDAN', 4), (14, 'KENYA', 0),
           (15, 'MOROCCO', 0), (16, 'MOZAMBIQUE', 0), (17,
                                                       'PERU', 1), (18, 'CHINA', 2), (19, 'ROMANIA', 3),
           (20, 'SAUDI ARABIA', 4), (21, 'VIETNAM', 2), (22,
                                                         'RUSSIA', 3), (23, 'UNITED KINGDOM', 3),
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
container_syllables_2 = ['CASE', 'BOX', 'BAG',
                         'JAR', 'PKG', 'PACK', 'CAN', 'DRUM']
brand = ['BRAND#11', 'BRAND#22', 'BRAND#33']

# picasso queries


def gen_query_picasso(template: str, query_id: int) -> tuple[list[str], list[tuple]]:
    queries: list[str] = []
    parameters: list[tuple] = []
    match query_id:
        case 1:
            for param1 in range(60, 121):
                queries.append(template.format(DELTA=param1))
                parameters.append((param1,))
        case 2:
            for s in range(1, 51):
                for t in type_syllables_3:
                    for region in regions:
                        queries.append(template.format(
                            SIZE=s, TYPE=t, REGION=region[1]))
                        parameters.append((s, t, region[1]))
        case 3:
            for seg in segments:
                for d3 in dates_03:
                    queries.append(template.format(SEGMENT=seg, DATE=d3))
                    parameters.append((seg, str(d3)))
        case 4:
            for param4 in dates_04:
                queries.append(template.format(DATE=param4))
                parameters.append((str(param4),))
        case 5:
            for param5 in it.product(regions, dates_05):
                queries.append(template.format(
                    REGION=param5[0][1], DATE=param5[1]))
                parameters.append((param5))
        case 6:
            for param6 in it.product(dates_05, discount, quantity):
                queries.append(template.format(
                    DATE=param6[0], DISCOUNT=param6[1], QUANTITY=param6[2]))
                parameters.append((param6))
        case 7:
            for nation_pair in it.permutations(nations, 2):
                queries.append(template.format(
                    NATION1=nation_pair[0][1], NATION2=nation_pair[1][1]))
                parameters.append((nation_pair))
        case 8:
            for param8 in it.product(nations, regions, type_syllables_3):
                if param8[0][2] == param8[1][0]:
                    queries.append(template.format(
                        NATION=param8[0][1], REGION=param8[1][1], TYPE=param8[2]))
                    parameters.append((param8))
        case 9:
            for param9 in colors:
                queries.append(template.format(COLOR=param9))
                parameters.append((param9,))
        case 10:
            for param10 in dates_10:
                queries.append(template.format(DATE=param10))
                parameters.append(((param10),))
        case 11:
            for param11 in nations:
                queries.append(template.format(
                    NATION=param11[1], FRACTION=0.0001 / SCALE_FACTOR))
                parameters.append((param11))
        case 12:
            for param12 in it.product(nations, regions, type_syllables_3):
                if param8[0][2] == param8[1][0]:
                    queries.append(template.format(
                        NATION=param8[0][1], REGION=param8[1][1], TYPE=param8[2]))
                    parameters.append((param8))
        case 13:
            for param13 in colors:
                queries.append(template.format(COLOR=param9))
                parameters.append((param9,))
        case 16:
            for param16 in dates_10:
                queries.append(template.format(DATE=param10))
                parameters.append(((param10),))
        case 17:
            for param17 in nations:
                queries.append(template.format(
                    NATION=param11[1], FRACTION=0.0001 / SCALE_FACTOR))
                parameters.append((param11))
        case 18:
            for param18 in range(312, 316):
                queries.append(template.format(QUANTITY=param18))
                parameters.append(((param18,)))
        case 19:
            for param19 in it.product(range(1, 11), range(10, 21), range(20, 31), brand):
                queries.append(template.format(QUANTITY1=param19[0], QUANTITY2=param19[1], QUANTITY3=param19[2],
                               BRAND1=param19[3][0], BRAND2=param19[3][1], BRAND3=param19[3][2]))  # Können die brands die gleichen sein?
                parameters.append((param19))
        case 20:
            for param20 in it.product(colors, dates_05, nations):
                queries.append(template.format(
                    COLOR=param20[0], DATE=param20[1], NATION=param20[2][1]))
                parameters.append((param20))
        case 21:
            for param21 in nations:
                queries.append(template.format(NATION=param21[1]))
                parameters.append((param21,))
    return queries, parameters


# picasso parameters values section

# ['EXTENDED_PRICE', 'RETAIL_PRICE', 'SUPPLY_COST', 'TOTAL_PRICE', 'C_ACCTBAL', 'S_ACCTBAL']


# l_quantity = [i for i in range(1,51)]
# l_extenedprice = l_quantity * p_retailprice
    # where p_retailprice is from the part with p_partkey = l_partkey
# extended_price =


supply_costs = [i for i in range(0, 105, 5)]

retail_prices = [i for i in range(0, 105, 5)]

total_prices = [i for i in range(0, 105, 5)]

c_acctbal = []

s_acctbal = []

# countries for the country example
countries = ['Vatican City', 'Christmas Island (Australia)', 'Tokelau (New Zealand)', 'Niue (New Zealand)', 'Norfolk Island (Australia)', 'Falkland Islands (UK)', 'Montserrat (UK)', 'Saint Helena, Ascension and Tristan da Cunha (UK)', 'Saint Pierre and Miquelon (France)', 'Saint Barthélemy (France)', 'Tuvalu', 'Wallis and Futuna (France)', 'Nauru', 'Cook Islands', 'Anguilla (UK)', 'Palau', 'British Virgin Islands (UK)', 'Saint Martin (France)', 'San Marino', 'Gibraltar (UK)', 'Monaco', 'Liechtenstein', 'Sint Maarten (Netherlands)', 'Marshall Islands', 'Northern Mariana Islands (US)', 'Turks and Caicos Islands (UK)', 'American Samoa (US)', 'Saint Kitts and Nevis', 'Faroe Islands (Denmark)', 'South Ossetia', 'Greenland (Denmark)', 'Guernsey (UK)', 'Bermuda (UK)', 'Dominica', 'Cayman Islands (UK)', 'Isle of Man (UK)', 'Andorra', 'U.S. Virgin Islands (US)', 'Tonga', 'Jersey (UK)', 'Antigua and Barbuda', 'Micronesia', 'Aruba (Netherlands)', 'Saint Vincent and the Grenadines', 'Grenada', 'Kiribati', 'Seychelles', 'Guam (US)', 'Curaçao (Netherlands)', 'Saint Lucia', 'Samoa', 'São Tomé and Príncipe', 'Abkhazia', 'Barbados', 'New Caledonia (France)', 'French Polynesia (France)', 'Vanuatu', 'Transnistria', 'Northern Cyprus', 'Iceland', 'Bahamas', 'Belize', 'Brunei', 'Cape Verde', 'Maldives', 'Malta', 'Western Sahara', 'Suriname', 'Montenegro', 'Luxembourg', 'Macau (China)', 'Solomon Islands', 'Guyana', 'Bhutan', 'Fiji', 'Comoros', 'Cyprus', 'Djibouti', 'Eswatini', 'Mauritius', 'Trinidad and Tobago', 'East Timor', 'Estonia', 'Equatorial Guinea', 'Bahrain', 'Kosovo', 'Guinea-Bissau', 'North Macedonia', 'Latvia', 'Slovenia', 'Lesotho', 'Albania', 'Gabon', 'Botswana', 'Gambia', 'Moldova', 'Jamaica', 'Qatar',
             'Lithuania', 'Namibia', 'Armenia', 'Puerto Rico (US)', 'Bosnia and Herzegovina', 'Uruguay', 'Mongolia', 'Georgia', 'Eritrea', 'Croatia', 'Panama', 'Kuwait', 'Mauritania', 'Oman', 'Liberia', 'Costa Rica', 'New Zealand', 'Ireland', 'Slovakia', 'Palestine', 'Lebanon', 'Norway', 'Finland', 'Denmark', 'Singapore', 'Paraguay', 'Republic of the Congo', 'El Salvador', 'Bulgaria', 'Central African Republic', 'Serbia', 'Nicaragua', 'Turkmenistan', 'Kyrgyzstan', 'Libya', 'Laos', 'Hong Kong (China)', 'Togo', 'Sierra Leone', 'Switzerland', 'Belarus', 'Austria', 'Hungary', 'Honduras', 'Israel', 'Azerbaijan', 'Tajikistan', 'United Arab Emirates', 'Greece', 'Sweden', 'Portugal', 'Dominican Republic', 'Czech Republic', 'Cuba', 'Jordan', 'Papua New Guinea', 'Belgium', 'Tunisia', 'Bolivia', 'Haiti', 'Burundi', 'Benin', 'Guinea', 'Rwanda', 'Zimbabwe', 'South Sudan', 'Ecuador', 'Cambodia', 'Guatemala', 'Netherlands', 'Senegal', 'Somalia', 'Chad', 'Romania', 'Zambia', 'Chile', 'Kazakhstan', 'Malawi', 'Sri Lanka', 'Mali', 'Taiwan', 'Burkina Faso', 'Syria', 'North Korea', 'Niger', 'Australia', 'Venezuela', 'Cameroon', 'Nepal', 'Ivory Coast', 'Madagascar', 'Saudi Arabia', 'Yemen', 'Mozambique', 'Ghana', 'Malaysia', 'Peru', 'Afghanistan', 'Angola', 'Ukraine', 'Uzbekistan', 'Morocco', 'Poland', 'Canada', 'Iraq', 'Uganda', 'Algeria', 'Argentina', 'Spain', 'Sudan', 'South Korea', 'Kenya', 'Colombia', 'Myanmar', 'Italy', 'Tanzania', 'South Africa', 'Thailand', 'United Kingdom', 'France', 'Germany', 'Turkey', 'Iran', 'Vietnam', 'Democratic Republic of the Congo', 'Egypt', 'Ethiopia', 'Philippines', 'Japan', 'Mexico', 'Russia', 'Bangladesh', 'Brazil', 'Nigeria', 'Pakistan', 'Indonesia', 'United States', 'India', 'China']

country_idxes = [i for i in range(1, 238)]


def generate_country_queries() -> list[str]:
    template = "SELECT country FROM users_extended WHERE country = '{COUNTRY}';"
    # template =
    # template = "SELECT country FROM users_extended WHERE country = '{COUNTRY}';"
    queries: list[str] = []
    for idx, country_idx in enumerate(country_idxes):
        queries.append(template.format(COUNTRY=country_idx))
    return queries
    print(generate_country_queries()[0])

# skew example


def generate_skew_queries() -> list[str]:
    template = "SELECT a,b FROM data WHERE a = '{A}' AND b = '{B}';"
    queries: list[str] = []
    for i in range(1, 20):
        for j in range(100, 14600, 100):
            queries.append(template.format(A=i, B=j))
    return queries


##########
# job
##########


####
# job queries gen
####


def generate_job_queries() -> list[str]:
    template_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/resources/job_parameterized/1d_qt.sql'
    queries = []

    # read the query template from the file
    with open(template_path, 'r') as file:
        query_template = file.read()

    # generate queries for each year in the range from 1880 to 2019
    for year in range(1880, 2020):
        query = query_template.replace('{YEAR}', str(year))
        queries.append(query)
    return queries


def generate_job_query15a() -> list[str]:
    country_codes = [
        '[ad]', '[ae]', '[af]', '[ag]', '[ai]', '[al]', '[am]', '[an]', '[ao]', '[ar]', '[as]', '[at]', '[au]',
        '[aw]', '[az]', '[ba]', '[bb]', '[bd]', '[be]', '[bf]', '[bg]', '[bh]', '[bi]', '[bj]', '[bl]', '[bm]',
        '[bn]', '[bo]', '[br]', '[bs]', '[bt]', '[bw]', '[by]', '[bz]', '[ca]', '[cd]', '[cg]', '[ch]', '[ci]',
        '[cl]', '[cm]', '[cn]', '[co]', '[cr]', '[cshh]', '[cu]', '[cv]', '[cy]', '[cz]', '[ddde]', '[de]',
        '[dk]', '[dm]', '[do]', '[dz]', '[ec]', '[ee]', '[eg]', '[er]', '[es]', '[et]', '[fi]', '[fj]', '[fo]',
        '[fr]', '[ga]', '[gb]', '[gd]', '[ge]', '[gf]', '[gg]', '[gh]', '[gi]', '[gl]', '[gn]', '[gp]', '[gr]',
        '[gt]', '[gu]', '[gw]', '[gy]', '[hk]', '[hn]', '[hr]', '[ht]', '[hu]', '[id]', '[ie]', '[il]', '[im]',
        '[in]', '[iq]', '[ir]', '[is]', '[it]', '[je]', '[jm]', '[jo]', '[jp]', '[ke]', '[kg]', '[kh]', '[ki]',
        '[kn]', '[kp]', '[kr]', '[kw]', '[ky]', '[kz]', '[la]', '[lb]', '[lc]', '[li]', '[lk]', '[lr]', '[ls]',
        '[lt]', '[lu]', '[lv]', '[ly]', '[ma]', '[mc]', '[md]', '[me]', '[mg]', '[mh]', '[mk]', '[ml]', '[mm]',
        '[mn]', '[mo]', '[mq]', '[mr]', '[mt]', '[mu]', '[mv]', '[mx]', '[my]', '[mz]', '[na]', '[nc]', '[ne]',
        '[ng]', '[ni]', '[nl]', '[no]', '[np]', '[nr]', '[nz]', '[om]', '[pa]', '[pe]', '[pf]', '[pg]', '[ph]',
        '[pk]', '[pl]', '[pm]', '[pr]', '[ps]', '[pt]', '[py]', '[qa]', '[ro]', '[rs]', '[ru]', '[rw]', '[sa]',
        '[sd]', '[se]', '[sg]', '[si]', '[sj]', '[sk]', '[sl]', '[sm]', '[sn]', '[so]', '[sr]', '[suhh]', '[sv]',
        '[sy]', '[sz]', '[td]', '[tf]', '[tg]', '[th]', '[tj]', '[tk]', '[tl]', '[tm]', '[tn]', '[to]', '[tr]',
        '[tt]', '[tv]', '[tw]', '[tz]', '[ua]', '[ug]', '[um]', '[us]', '[uy]', '[uz]', '[va]', '[ve]', '[vg]',
        '[vi]', '[vn]', '[xyu]', '[ye]', '[yucs]', '[za]', '[zm]', '[zw]'
    ]

    production_years = [
        production_year for production_year in range(1880, 2020)]

    template_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/resources/job_parameterized/selected/15a.sql'
    queries = []

    # read the query template from the file
    with open(template_path, 'r') as file:
        query_template = file.read()

    for country in country_codes:
        for year in production_years:
            query = query_template.replace('{COUNTRY_CODE}', country).replace(
                '{YEAR}', str(year))
            queries.append(query)
    return queries


def gen_custom_queries_aggregated() -> list[str]:
    template_query = """
                    SELECT p.p_name, l.l_partkey, AVG(p.p_retailprice)
                    FROM lineitem l
                    JOIN partsupp ps ON l.l_partkey = ps.ps_partkey
                    JOIN part p ON ps.ps_partkey = p.p_partkey
                    WHERE l.l_shipdate BETWEEN DATE '1992-02-01' AND DATE '{SHIP_DATE}'
                    GROUP BY l_partkey, p.p_name
                    HAVING COUNT(p.p_partkey) = 1;
                    """
    queries = []

    # Generate queries for each month between January 1992 and December 1998
    for year in range(1992, 1999):
        for month in range(1, 13):
            # Skip months before February 1992 and after December 1998
            if (year == 1992 and month < 2) or (year == 1998 and month > 12):
                continue
            # Format the date as "YYYY-MM-DD"
            ship_date = f"{year}-{month:02d}-01"
            query = template_query.replace('{SHIP_DATE}', ship_date)
            queries.append(query)

    return queries
