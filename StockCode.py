import pandas as pd
import FinanceDataReader as fdr
import json
import requests as rq
import pandas as pd
import time
import yfinance as yf
from datetime import timedelta, date, datetime
from tqdm import tqdm
import pymysql

print('DB connection')
conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()

df = fdr.StockListing('KRX') # KRX: 2,663 종목(=코스피+코스닥+코넥스)

df.head()

for idx in range(len(df)):
    code    = df['Code'].values[idx]
    name    = df['Name'].values[idx]
    market  = df['Market'].values[idx]
    # 주식종목 추출:개별주식 제외,펀드,Warrent 제외(ETF,ETN포함)
    if code[2:3]!='F' and code[2:3]!='G' and len(code) ==6 and name not in 'WR':
        #print(code + ","+name+","+market)
        sql = f"REPLACE INTO invest.stock_code (code, name)"\
              f" VALUES ('{code}', '{name}')"
              #f" VALUES ('"+code+"','"+name+"')"
        print(sql)
        cur.execute(sql)
        conn.commit()

# 상장폐지 종목 리스팅
df = fdr.StockListing('KRX-DELISTING') # 3천+ 종목 - 상장폐지 종목 전체

for idx in range(len(df)):
    code    = df['Symbol'].values[idx]
    name    = df['Name'].values[idx]
    market  = df['Market'].values[idx]
    # 주식종목 추출:개별주식 제외,펀드,Warrent 제외(ETF,ETN포함)
    if code[2:3]!='F' and code[2:3]!='G' and len(code) ==6 and name not in 'WR':
        #print(code + ","+name+","+market)
        sql = f"REPLACE INTO invest.stock_code (code, name)"\
              f" VALUES ('{code}', '{name}')"
              #f" VALUES ('"+code+"','"+name+"')"
        #print(sql)
        cur.execute(sql)
        conn.commit()

sector_code1 = [
    'G1010', 'G1510', 'G2010', 'G2020', 'G2030',
    'G2510', 'G2520', 'G2530', 'G2550', 'G2560',
    'G3010', 'G3020', 'G3030', 'G3510', 'G3520',
    'G4010', 'G4020', 'G4030', 'G4040', 'G4050',
    'G4510', 'G4520', 'G4530', 'G4535', 'G4540',
    'G5010', 'G5020', 'G5510'
]

sector_code1 = [
    'G10', 'G15', 'G20', 'G25', 'G30', 'G35', 'G40', 'G45', 'G50', 'G55'
]
#WICS 대분류
data_sector1 = pd.DataFrame();
#현재일자
cur_day = (datetime.today()-timedelta(1)).strftime('%Y%m%d')
for i in tqdm(sector_code1):
    # 날짜 하드코딩
    url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={cur_day}&sec_cd={i}'''
    print(url)
    data = rq.get(url).json()

    data_pd = pd.json_normalize(data['list'])
    #data_sector1 = data_sector1.append(data_pd)
    data_sector1 = pd.concat([data_sector1,data_pd], ignore_index = True)

    time.sleep(2)

for idx in range(len(data_sector1)):
    code = data_sector1['CMP_CD'].values[idx]
    sector_maj_code = data_sector1['IDX_CD'].values[idx]

    sql = f" UPDATE invest.stock_code SET sector_maj_code = '{sector_maj_code}'" \
          f"  WHERE code = '{code}'"
    print(sql)
    cur.execute(sql)
    conn.commit()
#WICS 중분류
sector_code2 = [
    'G1010', 'G1510', 'G2010', 'G2020', 'G2030',
    'G2510', 'G2520', 'G2530', 'G2550', 'G2560',
    'G3010', 'G3020', 'G3030', 'G3510', 'G3520',
    'G4010', 'G4020', 'G4030', 'G4040', 'G4050',
    'G4510', 'G4520', 'G4530', 'G4535', 'G4540',
    'G5010', 'G5020', 'G5510'
]

data_sector2 = pd.DataFrame();
for i in tqdm(sector_code2):
    # 날짝 하드코딩
    url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={cur_day}&sec_cd={i}'''
    data = rq.get(url).json()

    data_pd = pd.json_normalize(data['list'])
    #data_sector2 = data_sector2.append(data_pd)
    data_sector2 = pd.concat([data_sector2,data_pd], ignore_index = True)

    time.sleep(2)

for idx in range(len(data_sector2)):
    code = data_sector2['CMP_CD'].values[idx]
    sector_mid_code = data_sector2['IDX_CD'].values[idx]

    sql = f" UPDATE invest.stock_code SET sector_mid_code = '{sector_mid_code}'" \
          f"  WHERE code = '{code}'"
    print(sql)
    cur.execute(sql)
    conn.commit()
#WICS 소분류
sector_code3 = [
    'G101010', 'G101020', 'G151010', 'G151030', 'G151040',
    'G151050', 'G151060', 'G201010', 'G201020', 'G201025',
    'G201030', 'G201035', 'G201040', 'G201050', 'G201060',
    'G201065', 'G201070', 'G202010', 'G203010', 'G203020',
    'G203030', 'G203040', 'G203050', 'G251010', 'G251020',
    'G252040', 'G252050', 'G252060', 'G252065', 'G252070',
    'G253010', 'G253020', 'G255010', 'G255020', 'G255030',
    'G255040', 'G256010', 'G301010', 'G302010', 'G302020',
    'G302030', 'G303010', 'G351010', 'G351020', 'G351030',
    'G352010', 'G352020', 'G352030', 'G401010', 'G402010',
    'G403020', 'G403030', 'G403040', 'G404010', 'G404020',
    'G405020', 'G451020', 'G451030', 'G452010', 'G452015',
    'G452020', 'G452030', 'G452040', 'G453010', 'G453510',
    'G453520', 'G454010', 'G454020', 'G501010', 'G501020',
    'G502010', 'G502020', 'G502030', 'G502040', 'G502050',
    'G551010', 'G551020', 'G551030', 'G551050',
]

data_sector3 = pd.DataFrame();
for i in tqdm(sector_code3):
    url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={cur_day}&sec_cd={i}'''
    data = rq.get(url).json()

    data_pd = pd.json_normalize(data['list'])
    #data_sector3.append(data_pd)
    data_sector3 = pd.concat([data_sector3,data_pd], ignore_index = True)

    time.sleep(2)

for idx in range(len(data_sector3)):
    code = data_sector3['CMP_CD'].values[idx]
    sector_sma_code = data_sector3['IDX_CD'].values[idx]

    sql = f" UPDATE invest.stock_code SET sector_sma_code = '{sector_sma_code}'" \
          f"  WHERE code = '{code}'"
    print(sql)
    cur.execute(sql)
    conn.commit()

conn.close()
