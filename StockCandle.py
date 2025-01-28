import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mplfinance.original_flavor import candlestick2_ohlc
from matplotlib import font_manager, rc

conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()

sql = "SELECT date,open,high,low,close FROM invest.stock_price where date > STR_TO_DATE('20200101', '%Y%m%d') and code = '095660'"
cur.execute(sql)
result = cur.fetchall()
df = pd.DataFrame(list(result),columns=["date","open","high","low","close"])

fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(111)
# 한글 폰트 지정
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
# x-축 날짜
xdate = df.date.astype('str')
for i in range(len(xdate)): xdate[i] = xdate[i][2:] # 2020-01-01 => 20-01-01
# 종가 및 5,20,60,120일 이동평균
ax.plot(xdate, df['close'], label="종가",linewidth=0.7,color='k')
ax.plot(xdate, df['close'].rolling(window=5).mean(), label="평균5일",linewidth=0.7)
ax.plot(xdate, df['close'].rolling(window=20).mean(), label="평균20일",linewidth=0.7)
ax.plot(xdate, df['close'].rolling(window=60).mean(), label="평균60일",linewidth=0.7)
ax.plot(xdate, df['close'].rolling(window=120).mean(), label="평균120일",linewidth=0.7)
candlestick2_ohlc(ax,df['open'],df['high'],df['low'],df['close'], width=0.5, colorup='r', colordown='b')
fig.suptitle("캔들 스택 차트 예시")
ax.set_xlabel("날짜")
ax.set_ylabel("주가(원)")
ax.xaxis.set_major_locator(ticker.MaxNLocator(25)) # x-축에 보일 ticker 개수 ~20개이면 1달
ax.legend(loc=1) # legend 위치
plt.xticks(rotation = 45) # x-축 글씨 45도 회전

#plt.grid() # 그리드 표시
plt.show()