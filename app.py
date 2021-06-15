import twint
from binance import Client
from pprint import pprint
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import pytz
import timeit
from decouple import config

#CONFIG
binance_api_key = config('binance_api_key')
biance_secret_key = config('biance_secret_key')
client = Client(binance_api_key, biance_secret_key, tld='us')
crypto = 'DOGEUSDT' #cypto stock ticker
start_date='2021-1-1' #date to look at tweets from
post_twt_time_mn = 60 #minutes to look after tweet
pre_twt_time_mn = 5 #minutes to look before tweet 

#get tweets from twint
def get_tweets():
    c = twint.Config()
    c.Search = "doge AND -filter:replies"
    c.Verified = True
    c.Stats = True
    c.Filter_retweets = True
    c.Lang = 'en'
    c.Since = start_date
    c.Count = True
    c.Min_likes = 1
    c.Username = 'elonmusk'
    c.Pandas = True
    c.Hide_output = True

    twint.run.Search(c)

    tweets_df = twint.storage.panda.Tweets_df
    tweets_df.drop(tweets_df.columns.difference(['username','created_at','tweet']),1, inplace=True)
    tweets_df['created_at'] = pd.to_datetime(tweets_df['created_at'])
    tweets_df.set_index('created_at', inplace=True)
    tweets_df = tweets_df.tz_localize(None)
    print(tweets_df.index[0])
    return tweets_df

#get price history from 5 min before first tweet
def get_price_history(crypto_symbol, start_dt):
    new_start_dt = str(start_dt - timedelta(minutes=pre_twt_time_mn))
    end_dt = str(start_dt + timedelta(minutes=post_twt_time_mn))
    bars = client.get_historical_klines(crypto_symbol, '1m', new_start_dt, limit=1000)

    #delete unwanted columns and update timestamp to readable date
    for line in bars:
        line[0] = datetime.fromtimestamp(int(line[0]/1000))
        del line[5:]

    #create panda dataframe
    price_history_df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close'])
    price_history_df.set_index('date', inplace=True)
    price_history_df.index = pd.to_datetime(price_history_df.index, unit="ms")
    price_history_df.tz_localize(None)

    return price_history_df

#MAIN 
#Get dataframe of Tweets
tweets_df = get_tweets()
print('fetched tweets')
#get date of first tweet to pull price history
first_tweet_dt = tweets_df.index[-1]
print(tweets_df.tail())
print(tweets_df.dtypes)
print('fetching price history of', crypto)
price_history_df = get_price_history(crypto, first_tweet_dt)
#price_history_df = pd.read_pickle('price_history_df_w_roll')
print('fetched price history of ', crypto)
print(price_history_df.head())
print(price_history_df.index)


#set freq of df
price_history_df = price_history_df.asfreq('T')
#set forward looking indexer for roller
indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=60)
#get max rolling window and apply to df
print('setting max price of each following', post_twt_time_mn, 'minutes of every price' )
rolling_window = price_history_df['high'].rolling(indexer)
price_history_df['rolling_max_price'] = rolling_window.max()

#get rolling max and convert
print('setting max high dt of the following hour of each minute')
rolling_high_dt_srs = rolling_window.apply(lambda x: x.idxmax().to_datetime64(), raw=False)
print('set rolling high dt')
#convert returned series to datetime df
rolling_high_dt_df = rolling_high_dt_srs.to_frame()
rolling_high_dt_df.columns.values[0] = 'rolling_high_dt'
rolling_high_dt_df['rolling_high_dt'] = pd.to_datetime(rolling_high_dt_df['rolling_high_dt'])

#get average time between max and open within an hour BROKEN
rolling_high_dt_df['dt_col'] = rolling_high_dt_df.index
rolling_high_dt_df['time diff'] = pd.to_datetime(rolling_high_dt_df['rolling_high_dt']) - pd.to_datetime(rolling_high_dt_df['dt_col'])
avg_time_diff = rolling_high_dt_df['time diff'].mean().round('T')

#setup tweets df to merge 
tweets_df['rnd_dt'] = tweets_df.index.round('T')
tweets_df['close_dt'] = tweets_df['rnd_dt'] + timedelta(minutes=post_twt_time_mn)
tweets_df['pre_tweet_dt'] = tweets_df['rnd_dt'] - timedelta(minutes=pre_twt_time_mn)

#create column to get dt from avg_time_diff to merge with and get price it would sell at
tweets_df['avg_close_dt'] = tweets_df['rnd_dt'] + avg_time_diff

#create df to merge
rolling_max_price_df = price_history_df.drop(price_history_df.columns.difference(['date', 'rolling_max_price']),1)
rolling_high_dt_df = price_history_df.drop(price_history_df.columns.difference(['date', 'rolling_high_dt']), 1)
open_prices_df = price_history_df.drop(price_history_df.columns.difference(['date', 'open']),1)
closed_prices_df = price_history_df.drop(price_history_df.columns.difference(['date','close']),1)

results_df = pd.merge(tweets_df, open_prices_df, how='left', left_on='rnd_dt', right_on='date')
results_df = pd.merge(results_df, closed_prices_df, how='left', left_on='close_dt', right_on='date')
results_df = pd.merge(results_df, rolling_max_price_df, how='left', left_on='rnd_dt', right_on='date')
results_df = pd.merge(results_df, rolling_high_dt_df, how='left', left_on='rnd_dt', right_on='date')
results_df = pd.merge(results_df, closed_prices_df, how='left', left_on='avg_close_dt', right_on='date')

results_df['pct_diff'] = (results_df['close_y'].astype(float) / results_df['open'].astype(float) - 1)
results_df['profit'] = results_df['pct_diff'] * 1000
total_profit = "${:,.2f}".format(results_df['profit'].sum())
print(results_df.head())
print('total profit is: ', total_profit)
results_df.to_csv('results.csv', index=False, header=True)

#FIXED! Now to get it to Github