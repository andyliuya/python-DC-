import discord
from discord.ext import tasks, commands
import pandas as pd
import requests
from datetime import datetime, timedelta
import sqlite3

# 輸入你的機器人Token
TOKEN =  'MTI3NDE5NzY3MjE3NzQzODc0Mg.GNTH2q.RX4kye0KSDUTI-2Ybstr_RR0MfPOxMtGZy7Qg4'

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

def fetch_data(date: str) -> pd.DataFrame:
    """從指定的 URL 獲取三大法人買賣超數據"""
    url = f'http://www.twse.com.tw/fund/BFI82U?response=html&dayDate={date}'
    try:
        dfs = pd.read_html(url)
        df = dfs[0]
        df.columns = df.columns.droplevel()  # 刪除多層索引
        df.replace(',', '', regex=True, inplace=True)
        df.replace('--', pd.NA, inplace=True)
        df['年月日'] = date
        df['年月日'] = pd.to_datetime(df['年月日']).astype(str)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def find_previous_working_day(date: datetime) -> str:
    """查找最近的非假日工作日"""
    prev_day = date - timedelta(days=1)
    while prev_day.weekday() >= 5:  # 0=Monday, 1=Tuesday, ..., 5=Saturday, 6=Sunday
        prev_day -= timedelta(days=1)
    return prev_day.strftime('%Y%m%d')

def save_to_db(df: pd.DataFrame):
    """將數據保存到 SQLite 資料庫"""
    conn = sqlite3.connect('D:/trading_data.db')
    df.to_sql('institutional_data', conn, if_exists='append', index=False)
    conn.close()


def format_value(value):
    """格式化數據為簡潔表示法，以10^8為單位"""
    if pd.isna(value):
        return "N/A"
    value = float(value) / 1e8  # 除以10^8
    return f"{value:.2f}e"


# 新增查詢特定日期的功能
async def fetch_and_send_data(channel, date=None):
    """抓取數據並將其發送到指定的 Discord 頻道"""
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    try:
        # 如果有指定日期，使用指定日期的數據，否則使用當前日期
        df = fetch_data(date)
        
        if df.empty:
            # 如果當前日期沒有數據，則找前一個工作日
            prev_working_day = find_previous_working_day(datetime.strptime(date, '%Y%m%d'))
            df = fetch_data(prev_working_day)
            if df.empty:
                await channel.send("沒有可用的數據。")
                return
            else:
                # 使用前一個工作日的數據，更新報告標題
                report_date = prev_working_day
                await channel.send(f"**指定日期 ({date}) 無法獲取數據，顯示最近的交易日 ({prev_working_day})**")
        else:
            # 使用指定日期的數據
            report_date = date


        # 格式化數據為簡潔表示法
        df['買進金額'] = df['買進金額'].apply(format_value)
        df['賣出金額'] = df['賣出金額'].apply(format_value)
        df['買賣差額'] = df['買賣差額'].apply(format_value)
        

        # 格式化輸出
        output = f"**三大法人買賣金額統計 ({report_date})**\n"
        for _, row in df.iterrows():
            output += f"{row['單位名稱']}: {row['買賣差額']}\n"
        


        await channel.send(output)
    except Exception as e:
        await channel.send(f"獲取數據時出錯: {e}")



@client.event
async def on_ready():
    """當機器人完成啟動時執行"""
    print(f"目前登入身份 --> {client.user}")
    send_trading_data.start()

# 當機器人收到消息時觸發
@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('三大法人買賣超'):
        try:
            # 提取日期
            date_str = message.content.split(' ')[1]
            if len(date_str) == 8 and date_str.isdigit():
                await fetch_and_send_data(message.channel, date_str)
            else:
                await message.channel.send("請使用正確的日期格式，例如 20240814")
        except IndexError:
            await fetch_and_send_data(message.channel)  # 如果沒有指定日期，抓取當日數據

# @tasks.loop(hours=24)
# async def send_trading_data():
#     """每天定時發送三大法人買賣超數據"""
#     now = datetime.now()
#     if now.weekday() < 5 and now.hour == 13 and now.minute == 0:
#         for guild in client.guilds:
#             for channel in guild.text_channels:
#                 await fetch_and_send_data(channel)

client.run(TOKEN)




