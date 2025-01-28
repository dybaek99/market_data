# ECOS 국고채3년/회사채 수익률 가져오기 및 TBILL 5yer 가져오기
import requests
from bs4 import BeautifulSoup
from datetime import timedelta, date, datetime
import pymysql
import pandas as pd
import numpy as np
import yfinance as yf

print('DB connection')
conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()
# 코드는 ecos open API 개발가이드-통계코드검색
# 20210927
FromDate = date.today() - timedelta(1)
ToDate   = date.today() - timedelta(1)
FromDate2= date.today()
ToDate2  = date.today()
# 최대 처리 개수는 5000개
#url = 'https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/060Y001/DD/'+ FromDate.strftime("%Y%m%d")+'/'+ToDate.strftime("%Y%m%d")+ '/010200000/'
url = 'https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/817Y002/D/20231030/20231030/010200000/'
print(url)
#xml = requests.get('https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/060Y001/DD/19981113/20091231/010200000/')
#xml = requests.get('https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/060Y001/DD/20100101/20210830/010200000/')
#xml = requests.get('https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/731Y003/D/20211214/20231030/010200000/')
# 회사채 3년010320000

xml = requests.get(url)
with open('asdf.txt', 'a+', encoding="UTF-8") as t:
    t.write(xml.text)
# 커맨드창에서 chcp 65001(UTF-8)로 변환 필요(최초는 949)
with open("asdf.txt","r",encoding="UTF-8") as fp:
    soup = BeautifulSoup(fp, 'html.parser')

for row in soup.findAll('row'):
    #print(row.time.text, row.data_value.text)
    date = datetime.strptime(row.time.text,'%Y%m%d')
    date = date.strftime('%Y-%m-%d')
    sql = f"REPLACE INTO invest.bond_interest_rate (code, date, rate)"\
          f" VALUES ('KR3TB', '{date}','{row.data_value.text}')"
    cur.execute(sql)
    conn.commit()
#url = 'https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/060Y001/DD/'+ FromDate.strftime("%Y%m%d")+'/'+ToDate.strftime("%Y%m%d")+ '/010320000/'
url = 'https://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/817Y002/D/20231031/20240712/010320000/'
print(url)
xml = requests.get(url)
with open('qwer.txt', 'a+', encoding="UTF-8") as t:
    t.write(xml.text)
# 커맨드창에서 chcp 65001(UTF-8)로 변환 필요(최초는 949)
with open("qwer.txt","r",encoding="UTF-8") as fp:
    soup = BeautifulSoup(fp, 'html.parser')

for row in soup.findAll('row'):
    #print(row.time.text, row.data_value.text)
    date = datetime.strptime(row.time.text,'%Y%m%d')
    date = date.strftime('%Y-%m-%d')
    sql = f"REPLACE INTO invest.bond_interest_rate (code, date, rate)"\
          f" VALUES ('KR3CBBBB-', '{date}','{row.data_value.text}')"
    cur.execute(sql)
    conn.commit()

# ^GSPC : S&P500, T-bIIl 5년 ^FVX crud oil CL=F gold GC=F 1962-01-01
code = '^FVX'
print(code)
#df = yf.download(code, start=FromDate2, end=ToDate2)
df = yf.download(code, start=FromDate2, end=ToDate2)
#df = yf.download(code, start="2021-10-16", end="2023-10-20")
df = df.reset_index()
print(df)
df.head()
# 데이타 생성
for idx in range(len(df)):
    date      = df['Date'].values[idx]
    rate      = df['Close'].values[idx]
    sql = f"REPLACE INTO invest.bond_interest_rate (code, date, rate)"\
          f" VALUES ('{code}', '{date}','{rate}')"
    cur.execute(sql)
    conn.commit()

conn.close()