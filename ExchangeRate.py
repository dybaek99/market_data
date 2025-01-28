# ECOS 환율 가져오기
import requests
from bs4 import BeautifulSoup
from datetime import timedelta, date, datetime
import pymysql

print('DB connection')
conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()
# 코드는 ecos open API 개발가이드-통계코드검색
# 20210927
FromDate = date.today() - timedelta(1)
ToDate   = date.today() - timedelta(1)
# 최대 처리 개수는 5000개
#url = 'http://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/731Y003/D/'+ FromDate.strftime("%Y%m%d")+'/'+ToDate.strftime("%Y%m%d")+ '/0000003/'
url = 'http://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/731Y003/D/20231031/20240712/0000003/'
print(url)
#xml = requests.get('http://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/036Y003/DD/20211214/20231030/0000003/')
#ecos 코드 개편에 따른 변경
#xml = requests.get('http://ecos.bok.or.kr/api/StatisticSearch/W9W3357LS1EA7R4T62M5/xml/kr/1/5000/731Y003/D/20211214/20231030/0000003/')

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
    sql = f"REPLACE INTO invest.exchange_rate (code, date, rate)"\
          f" VALUES ('USD', '{date}','{row.data_value.text}')"
    cur.execute(sql)
    conn.commit()

conn.close()