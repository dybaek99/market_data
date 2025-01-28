import plotly.graph_objects as go
import plotly.subplots as ms
import pandas_datareader as web
import pymysql
import pandas as pd

conn = pymysql.connect(host='localhost', user='invest',
                        password='invest', db='invest', charset='utf8')

cur = conn.cursor()

sql = "SELECT date,open,high,low,close,volume FROM invest.stock_price where date > STR_TO_DATE('20210429', '%Y%m%d') and code = '095660'"
cur.execute(sql)
result = cur.fetchall()
df = pd.DataFrame(list(result),columns=["date","open","high","low","close","volume"])
# 캔들 차트 객체 생성
candle = go.Candlestick(
    x=df['date'],
    open=df['open'],
    high=df['high'],
    low=df['low'],
    close=df['close'],
    increasing_line_color = 'red', # 상승봉 스타일링
    decreasing_line_color = 'blue' # 하락봉 스타일링
)
# 바 차트(거래량) 객체 생성
volume_bar = go.Bar(x=df['date'], y=df['volume'])

fig = ms.make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)

fig.add_trace(candle, row=1, col=1)
fig.add_trace(volume_bar, row=2, col=1)

fig.update_layout(
    title= 'stock price',
    yaxis1_title='Stock Price',
    yaxis2_title='Volume',
    xaxis2_title='periods',
    xaxis1_rangeslider_visible=False,
    xaxis2_rangeslider_visible=True,
)

fig.show()