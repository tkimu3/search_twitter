import sys
sys.path.append('../')
import config
import tweepy
import os
import json
import codecs

import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta, timezone
import locale

# Define how many days to look back tweets
# 取得対象のツイートの時間幅を指定する N=6の場合は実行前日までの6日間（最大）
N = 3

# Define the number of tweets limits to get
# 指定した時間幅に、limitで指定した件数以上のツイートがあってもlimit以上は取得しない
LIMIT = 50000

# Define the timezone
JST = timezone(timedelta(hours=9))

# 検索ワード
SEARCH_TERM = "CVE"

# Clientを返す認証関数
def auth():
    # API KEY
    CK = config.CK
    CS = config.CS
    AT = config.AT
    AS = config.AS
    BT = config.BT

    # wait_on_rate_limit = True とするとレートリミットを確認しながら取得処理を行う
    client = tweepy.Client(bearer_token = BT,\
                           consumer_key = CK,\
                           consumer_secret = CS,\
                           access_token = AT,\
                           access_token_secret = AS,\
                           wait_on_rate_limit = True)

    return client

def time_span(days, timezone):
    # 設定された時間幅(days)とTimezoneに基づいて、当該Timezoneにおけるツイート取得対象時刻の始点と終点(start_time_tweepy, end_time_tweepy)をTweepy向けに指定し、文字列として返す
    # iso形式のUTC時間で指定しないとtweepy正しく時間指定ができない模様
    # 取得開始日(start_date)と取得終了日(end_date)を、文字列として返す


    now = datetime.now(tz = timezone) #JST
    start_time = now - timedelta(days=days)
    start_time = start_time.replace(hour = 0, minute = 0, second=0, microsecond=0)
    end_time = now.replace(hour = 0, minute = 0, second=0, microsecond=0)
    start_time_tweepy = str(start_time.isoformat())
    end_time_tweepy = str(end_time.isoformat())
    # end_time_tweepy = str(end_time.isoformat()) +'+09:00' #JST
    # start_time_tweepy = str(start_time.isoformat())+'+09:00' #JST

    start_date = start_time.strftime("%Y%m%d")
    # end_date should be -1 day because all the tweets are included in the day before today
    end_date = (end_time - timedelta(days = 1)).strftime("%Y%m%d")
    return start_time_tweepy, end_time_tweepy, start_date, end_date

# print(time_span(N))

def put_tweets_in_df(search_term, client, start_time_tweepy, end_time_tweepy, limit):
    df_tweet = pd.DataFrame()
    for tweet in tweepy.Paginator(client.search_recent_tweets,\
                                query = search_term,\
                                start_time=start_time_tweepy,\
                                end_time=end_time_tweepy,\
                                tweet_fields=['id','created_at','text','author_id','public_metrics',],\
                                max_results = 100).flatten(limit = limit):
        df_tweet = pd.concat([df_tweet, pd.DataFrame({'created_at': pd.Series([tweet.created_at]),\
                                                  'id': pd.Series([tweet.id]),\
                                                  'text': pd.Series([tweet.text]),\
                                                  'author_id': pd.Series([tweet.author_id]),\
                                                  'edit_history_tweet_ids': pd.Series([tweet.edit_history_tweet_ids]),\
                                                  'public_metrics': pd.Series([tweet.public_metrics])\
                                                 },\
                                                )])

    return df_tweet


client = auth()
result = time_span(N, JST)
start_time_tweepy = result[0]
end_time_tweepy = result[1]
start_date = result[2]
end_date = result[3]

# print(type(start_time))
df = put_tweets_in_df(SEARCH_TERM, client, start_time_tweepy, end_time_tweepy, LIMIT)
filename = start_date + "-" + end_date + ".csv"
df.to_csv(filename, index=False,header=True)

print(f"Time Span : from {start_date} to  {end_date}\n")
print(f"The number of tweets during the time span: {len(df)}\n")
print(f"df.head() : {df.head()}")
print(f"df.tail() : {df.tail()}")
print(f"filename: {filename}")

