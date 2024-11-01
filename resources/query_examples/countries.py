import psycopg2 as pg 
import pandas as pd
import numpy as np
import random
import math

url = 'https://de.wikipedia.org/wiki/Liste_der_L%C3%A4nder_und_Territorien_nach_Einwohnerzahl'
html = pd.read_html(url)
df = max(html, key=len)
# nur staat und einwohnerzahl
df = df[['Staat', 'Einwohnerzahl (2024)']]
# welt und eu rausnehemn 
df = df[df['Staat'] != 'Welt']
df = df[df['Staat'] != 'Europäische Union']
# columns umbenenen
df.columns = ['country', 'population']
# tausender seperatoren removen
df['population'] = df['population'].str.replace('.', '') 
df['population'] = df['population'].astype(int)
# durch tausend teilen wegen einfachheit
#df['population'] = np.where(df['population'] < 1000,
#                            df['population'],
#                           df['population'].astype(int) // 1000)'
df['population'] = df['population'].apply(lambda x: math.ceil(x/1000))
# clean up country names 
df['country'] = df['country'].str.replace('\[.*?\]', '')
df['country'] = df['country'].str.strip()
df['country'] = (df['country']
                .str.replace('\[.*?\]', '', regex=True)      
                .str.replace('\xa0', ' ', regex=True)        
                .str.replace('\s+', ' ', regex=True)         
                .str.strip()) 
# reset index
df = df.reset_index(drop=True)
print(df.tail(10))

