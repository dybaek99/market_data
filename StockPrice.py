import pandas as pd
from pykrx import stock
import pymysql
from datetime import timedelta, date
import numpy as np


print('DB connection')
conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

#start_date = date(2021,12, 14)
#end_date = date(2023,10, 06)
#20230830
start_date = date.today() - timedelta(179)
end_date   = date.today()
print(start_date)
print(end_date  )
for single_date in daterange(start_date, end_date):
    date1 = single_date.strftime("%Y-%m-%d")
    date2 = single_date.strftime("%Y%m%d")
    # 주식 종가
    print(date2)
    df1 = stock.get_market_ohlcv_by_ticker(date2,"ALL")

    # 주식 거래량
    df2 = stock.get_market_cap_by_ticker(date2,"ALL")

    # 주식 PER,PBR 등
    df3 = stock.get_market_fundamental_by_ticker(date2,"ALL")
    # 주식 종가,거래량,PER,PBR 등 통합
    df5 = pd.concat([df1,df2, df3],axis=1,join = 'outer')
    df5 = df5.rename_axis('티커').reset_index()
    df5 = df5.replace([np.inf, -np.inf], np.nan)
    df5.fillna(0, inplace=True)
    ## 데이타 생성
    for idx in range(len(df5)):
        code      = df5['티커'].values[idx]
        date      = date1
        open      = df1['시가'].values[idx]
        high      = df1['고가'].values[idx]
        low       = df1['저가'].values[idx]
        close     = df1['종가'].values[idx]
        diff      = df1['거래량'].values[idx]
        volume    = df1['거래대금'].values[idx]
        stock_num = df5['상장주식수'].values[idx]
        BPS       = df5['BPS'].values[idx]
        EPS       = df5['EPS'].values[idx]
        DPS       = df5['DPS'].values[idx]
        PBR       = df5['PBR'].values[idx]
        PER       = df5['PER'].values[idx]
        DIV_YLD   = df5['DIV'].values[idx]
        # 휴일인 경우 제외
        if open!=0 or high!=0 or low!=0 or close!=0:
          sql = f"REPLACE INTO invest.stock_price (code, date, open, high, low, close, diff, volume, stock_num, BPS, EPS, DPS, PBR, PER, DIV_YLD)"\
                  f" VALUES ('{code}', '{date}','{open}', '{high}','{low}', '{close}','{diff}', '{volume}','{stock_num}', '{BPS}','{EPS}', '{DPS}','{PBR}', '{PER}','{DIV_YLD}')"
                  #f" VALUES ('"+code+"','"+name+"')"
          #print(sql)
          cur.execute(sql)
          conn.commit()


    df4 = stock.get_etf_ohlcv_by_ticker(date2)
    df4 = df4.rename_axis('티커').reset_index()
    for idx in range(len(df4)):
        code      = df4['티커'].values[idx]
        date      = date1
        open      = df4['시가'].values[idx]
        high      = df4['고가'].values[idx]
        low       = df4['저가'].values[idx]
        close     = df4['종가'].values[idx]
        diff      = df4['거래량'].values[idx]
        volume    = df4['거래대금'].values[idx]
        # 휴일인 경우 제외
        if open!=0 or high!=0 or low!=0 or close!=0:
            sql = f"REPLACE INTO invest.stock_price (code, date, open, high, low, close, diff, volume, stock_num, BPS, EPS, DPS, PBR, PER, DIV_YLD)"\
                  f" VALUES ('{code}', '{date}','{open}', '{high}','{low}', '{close}','{diff}', '{volume}',0,0,0,0,0,0,0)"
            cur.execute(sql)
            conn.commit()

conn.close()
