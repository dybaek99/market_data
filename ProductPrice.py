import pymysql
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import timedelta, date, datetime
# 20210927
FromDate = date.today()
ToDate   = date.today()

print('DB connection')
conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()

# ^GSPC : S&P500, T-bIIl 5년 ^FVX crud oil CL=F gold GC=F
code = 'CL=F'
#df1 = yf.download(code, start=FromDate, end=ToDate) # 1962-01-01
df1 = yf.download(code, start="2024-07-11", end="2024-07-14") # 1962-01-01
df1 = df1.reset_index()
print(df1)

for idx in range(len(df1)):
    date       = df1['Date'].values[idx]
    open       = df1['Open'].values[idx]
    high       = df1['High'].values[idx]
    low        = df1['Low'].values[idx]
    close      = df1['Adj Close'].values[idx]
    volume     = df1['Volume'].values[idx] 

    sql = f"REPLACE INTO invest.product_price (code , date , open , high , low , close , volume )"\
          f" VALUES ('{code}', '{date}','{open}', '{high}','{low}', '{close}','{volume}')"
    print(sql)
    cur.execute(sql)
    conn.commit()

# ^GSPC : S&P500, T-bIIl 5년 ^FVX crud oil CL=F gold GC=F
code = 'GC=F'
#df2 = yf.download(code, start=FromDate, end=ToDate) # 1962-01-01
df2 = yf.download(code, start="2024-07-11", end="2024-07-14") # 1962-01-01
df2 = df2.reset_index()
print(df2)

for idx in range(len(df2)):
    date       = df2['Date'].values[idx]
    open       = df2['Open'].values[idx]
    high       = df2['High'].values[idx]
    low        = df2['Low'].values[idx]
    close      = df2['Adj Close'].values[idx]
    volume     = df2['Volume'].values[idx] 

    sql = f"REPLACE INTO invest.product_price (code , date , open , high , low , close , volume )"\
          f" VALUES ('{code}', '{date}','{open}', '{high}','{low}', '{close}','{volume}')"
    print(sql)
    cur.execute(sql)
    conn.commit()

conn.close()