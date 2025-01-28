#KOSPI200 지수 가져오기
import requests
import pandas as pd
import numpy as np
from io import BytesIO
import pymysql
from datetime import timedelta, date, datetime
import yfinance as yf
from dateutil.parser import parse

print('DB connection')
conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()
# 20210927
# 200일 정도로 제한 FromDate/FromDate2/FromDate3 변경 필요
FromDate = date.today() - timedelta(300)
ToDate   = date.today() - timedelta(1)
FromDate2 = date.today() - timedelta(300)
ToDate2   = date.today() - timedelta(1)
# 웹브라우저-기타코드-개발자도구-네트워크-generate.cmd
gen_req_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
query_str_parms = {
    'indIdx': '1',
    'indIdx2': '028',
    'strtDd': FromDate.strftime("%Y%m%d") ,#FromDate.strftime("%Y%m%d"), # '19950204 + FromDate.strftime("%Y%m%d")+'/'+ToDate.strftime("%Y%m%d")
    'endDd': ToDate.strftime("%Y%m%d"),
    'share': '2',
    'money': '3',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT00301'
}
headers = {
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': '****' #generate.cmd에서 찾아서 입력하세요
}
r = requests.get(gen_req_url, query_str_parms, headers=headers)
# 웹브라우저-기타코드-개발자도구-네트워크-download.cmd
gen_req_url = 'http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd'
form_data = {
    'code': r.content
}
r = requests.post(gen_req_url, form_data, headers=headers)
df1 = pd.read_excel(BytesIO(r.content))
print('KRX crawling completed :', df1)
# 사이트 개편에 따른 PER 크롤링 변경
#gen_req_url = 'http://global.krx.co.kr/contents/COM/GenerateOTP.jspx?name=fileDown&filetype=xls&url=GLB/05/0501/0501100100/glb0501100100_02&type=kospi&period_selector=day&fromdate=20211214&todate=20231031&pagePath=%2Fcontents%2FGLB%2F05%2F0501%2F0501100100%2FGLB0501100100.jsp'
# 1년 단위로 실행
##gen_req_url = 'http://global.krx.co.kr/contents/COM/GenerateOTP.jspx?name=fileDown&filetype=xls&url=GLB/05/0501/0501100100/glb0501100100_02&type=kospi&period_selector=day&fromdate='+FromDate.strftime("%Y%m%d")+'&todate='+ToDate.strftime("%Y%m%d")+'&pagePath=%2Fcontents%2FGLB%2F05%2F0501%2F0501100100%2FGLB0501100100.jsp'
#print(gen_req_url)
#query_str_parms = {
#    'name': 'fileDown',
#    'filetype': 'xls',
#    'url': 'GLB/05/0501/0501100100/glb0501100100_02',
#    'type': 'kospi',
#    'period_selector': 'day',
#    'pagePath': '/contents/GLB/05/0501/0501100100/GLB0501100100.jsp'
#}
#print('KRX crawling1')

#r = requests.get(gen_req_url, query_str_parms, headers=headers)
#gen_req_url = 'http://file.krx.co.kr/download.jspx'
#form_data = {
#    'code': r.content
#}

#r = requests.post(gen_req_url, form_data, headers=headers)
#df2 = pd.read_excel(BytesIO(r.content))
# FromDate3= date.today() - timedelta(1)
# ToDate3  = date.today()
FromDate3 = date.today() - timedelta(300)
ToDate3   = date.today() - timedelta(1)
print(FromDate3)
print(ToDate3)
#FromDate3 = date.today() - timedelta(300)
#ToDate3 = date.today() - timedelta(101)
df2 = pd.DataFrame()

while FromDate3 <= ToDate3:
    FromDate3_1 = FromDate3.strftime("%Y%m%d")
    FromDate3_2 = FromDate3.strftime("%Y/%m/%d")

    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    # gen_otp_stk = {
    #
    #     'searchType': 'A',
    #     'idxIndMidclssCd': '02',
    #     'trdDd': FromDate3_1,
    #     'csvxls_isNo': 'false',
    #     'name': 'fileDown',
    #     'url': 'dbms/MDC/STAT/standard/MDCSTAT00701'
    # }
    gen_otp_stk = {
        'searchType': 'A',  # Search type ('A' for all)
        'idxIndMidclssCd': '02',  # Index or industry classification code ('02' for KOSPI200)
        'trdDd': FromDate3_1,  # Date for which you want to retrieve data (YYYYMMDD format)
        'csvxls_isNo': 'false',  # 'false' for CSV format, 'true' for XLS format
        'name': 'fileDown',  # Name for the download request
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00701'  # URL for PER data
    }
    # headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    # otp_stk = requests.post(gen_otp_url, gen_otp_stk, headers=headers).text
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Request OTP
    otp_response = requests.post(gen_otp_url, gen_otp_stk, headers=headers)
    otp_stk = otp_response.text.strip()
    print(otp_stk)

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    print(down_url)
    down_sector_stk = requests.post(down_url, {'code': otp_stk}, headers=headers)
    print(down_sector_stk)
    df2_1 = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
    df2_1.head()
    print(df2_1.head())
    df2_1 = df2_1.loc[[1], ['PER']]
    df2_1.insert(0, '일자', FromDate3_2)
    print(df2_1)
    df2 = df2._append(df2_1, ignore_index=True)
    print(df2)
    FromDate3 = FromDate3 + timedelta(1)

# 주식 종가,거래량,PER,PBR 등 통합
#df5 = pd.concat([df1,df2],axis=1,join = 'outer')
df5 = pd.merge(df1,df2,how = 'inner', on = '일자')
print(df5)
#df5 = df5.rename_axis('일자').reset_index()
df5 = df5.replace([np.inf, -np.inf], np.nan)
df5 = df5.fillna(0)
df5.drop_duplicates(['일자'])
#print(df5)
## 데이타 생성
for idx in range(len(df5)):
    code       = 'A028'
    date       = df5['일자'].values[idx]
    open       = df5['시가'].values[idx]
    high       = df5['고가'].values[idx]
    low        = df5['저가'].values[idx]
    close      = df5['종가'].values[idx]
    diff       = df5['거래량'].values[idx]
    volume     = df5['거래대금'].values[idx]
    market_cap = df5['상장시가총액'].values[idx]
    market_cap = str(market_cap).replace("-","0")
    PER        = df5['PER'].values[idx]
    PER        = str(PER).replace("-","0")

    sql = f"REPLACE INTO invest.index_price (code , date , open , high , low , close , diff , volume , market_cap ,PER)"\
          f" VALUES ('{code}', '{date}','{open}', '{high}','{low}', '{close}','{diff}', '{volume}','{market_cap}', '{PER}')"
    print(sql)
    cur.execute(sql)
    conn.commit()

# ^GSPC : S&P500, T-bIIl 5년 ^FVX crud oil CL=F gold GC=F
code = '^GSPC'
df6 = yf.download(code, start=FromDate2, end=ToDate2) # 1962-01-01
#df6 = yf.download(code, start="2021-10-15", end="2023-10-31") # 1962-01-01
df6 = df6.reset_index()
#print(df6)

for idx in range(len(df6)):
    date       = df6['Date'].values[idx]
    open       = df6['Open'].values[idx]
    high       = df6['High'].values[idx]
    low        = df6['Low'].values[idx]
    close      = df6['Adj Close'].values[idx]
    diff       = 0
    volume     = df6['Volume'].values[idx]
    market_cap = 0
    PER        = 0


    sql = f"REPLACE INTO invest.index_price (code , date , open , high , low , close , diff , volume , market_cap ,PER)"\
          f" VALUES ('{code}', '{date}','{open}', '{high}','{low}', '{close}','{diff}', '{volume}','{market_cap}', '{PER}')"
    print(sql)
    cur.execute(sql)
    conn.commit()
conn.close()