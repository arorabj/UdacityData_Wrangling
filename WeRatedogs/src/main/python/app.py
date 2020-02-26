#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import json
import requests
import tweepy
import configparser as cp

import matplotlib.pyplot as plt
from matplotlib import cm

# pd.options.display.max_rows
# pd.set_option('display.max_colwidth', -1)

config_param= cp.RawConfigParser()
config_param.read('../../../resources/application.properties')
env = 'dev'

downlodDir=config_param.get(env,'file_download_path')

urlTweetImagePredictionData = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
tweetArchiveFile = downlodDir + 'twitter-archive-enhanced.csv'
tweetDataFilename = downlodDir   + 'downloads/' + 'tweet_json.txt'
tweetCleanFile =  downlodDir + 'twitter_archive_clean.csv'

consumerApiToken = config_param.get(env,'CONSUMER_KEY')
consumerApiSecretKey = config_param.get(env,'CONSUMER_SECRET')
apiToken = config_param.get(env,'OAUTH_KEY')
apiSecretKey = config_param.get(env,'OAUTH_SECRET')

# # Download file image-predictions.tsv programatically
fileImagePredictions = requests.get(urlTweetImagePredictionData)
imagePredictionFilename =  downlodDir +'downloads/' +urlTweetImagePredictionData.split('/')[-1]

with open(imagePredictionFilename, 'wb') as myfile:
    myfile.write(fileImagePredictions.content)

# # Download file twitter-archive-enhanced.csv
#fileImagePredictions = requests.get(urlTweetArchiveFile)
# with open(tweetArchiveFile, 'wb') as myfile:
#     myfile.write(fileImagePredictions.content)

# # Set Twitter authentication

auth = tweepy.OAuthHandler(consumerApiToken, consumerApiSecretKey)
auth.set_access_token(apiToken, apiSecretKey)
api = tweepy.API(auth_handler=auth,
                 wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

# # Read twitter-archive-enhanced.csv in dataframe to get tweetid
df_tweetFile = pd.read_csv(tweetArchiveFile)

# # Take tweet id values in list
tweetList = df_tweetFile.tweet_id.values
cntr = 0

# # Get data from twitter for every tweet and write in file tweet_json.txt
# list to capture exceptions
exceptionTweetIdList = list()
with open(tweetDataFilename, 'w') as myfile:
    for tweetid in tweetList:
        cntr += 1
        try:
            tweet = api.get_status(tweetid, tweet_mode='extended')
            json.dump(tweet._json, myfile)
            myfile.write('\n')
        except Exception as e:
            print('Data not found for tweet :' + str(cntr) + ':' + str(tweetid))
            print(e)
            exceptionTweetIdList.append(tweetid)

# # read above file line by line to extract the data in dataframe
df_raw = list()
with open(tweetDataFilename) as myfile:
    line = myfile.readlines()
    for i in line:
        a = json.loads(i)
        tweet_id = a['id']
        retweet_count = a['retweet_count']
        favourite_count = a['favorite_count']

        df_raw.append({"tweet_id": tweet_id,
                       "retweet_count": retweet_count,
                       "favourite_count": favourite_count,
                       })

df_json = pd.DataFrame(df_raw)

# # Assessing
df_tweetFile.info()
df_tweetFile.describe()
df_tweetFile.rating_numerator.value_counts()
df_tweetFile.rating_denominator.value_counts()
df_tweetFile.source.value_counts()
df_TweetFileArchiveClean = df_tweetFile

# # Quality Issues Identified :
# 1) Ignore retweets by selecting NaN columns basis on in_reply_to_status_id and  reduce the data to original tweets only.
# 2) Ignore retweets by selecting Nan Columns basis on retweeted_status_id and reduce the data to original tweets only.
# 3) Drop unused column like  column in_reply_to_status_id, retweeted_status_user_id
# 4) Correct Timestamp column format to time
# 5) Source column can be removed with unwanted text
# 6) Tweets have denominator not equal to 10 which seems to be invalid rows so those should be corrected using text if possible else removed from dataframe
# 7) When we only look at tweets with rating_denominator of 10, there are 12 tweets with rating_numerator >= 15
# 8) Invalid names like a, in, an should be chnaged to None

# # Tideness issues 
# 1) When all rating_denominators are the same, this column is no longer needed.
# 2) There are four columns for dog stages which can be accomodated in one
# 3) no need of separate json data as this can be combined to one dataframe which holds archive data

# # Cleaning Quality Issues 

# Define
# 
#     Ignore retweets by selecting NaN columns basis oo in_reply_to_status_id and reduce the data to original tweets only
# 
# Code
df_TweetFileArchiveClean = df_TweetFileArchiveClean[df_TweetFileArchiveClean.in_reply_to_status_id.isna()]

# Define
# 
#     Ignore retweets by selecting Nan Columns basis on retweeted_status_id and reduce the data to original tweets only.
# 
# Code
df_TweetFileArchiveClean = df_TweetFileArchiveClean[df_TweetFileArchiveClean.retweeted_status_id.isna()]

# Define
# 
#     Drop unused column like column in_reply_to_status_id, retweeted_status_user_id
#     
# Code
df_TweetFileArchiveClean = df_TweetFileArchiveClean.drop(
    ['in_reply_to_status_id', 'retweeted_status_user_id', 'retweeted_status_id', 'in_reply_to_user_id',
     'retweeted_status_timestamp'], axis=1)


# Define
#     
#     Correct Timestamp column format to time
#     
# Code
df_TweetFileArchiveClean.timestamp = pd.to_datetime(df_TweetFileArchiveClean['timestamp'], format="%Y-%m-%d %H:%M:%S")

# Define
# 
#     Source column can be removed with unwanted text
#     
# Code
df_TweetFileArchiveClean.source = df_TweetFileArchiveClean.source.str.extract('^<a.+>(.+)</a>$')

# Define
# 
#     Tweets have denominator not equal to 10 which seems to be invalid rows so those should be corrected using text if possible else removed from dataframe
#     
# Code
tweet_id_list = [740373189193256964, 716439118184652801, 682962037429899265, 666287406224695296]
df_TweetFileArchiveClean.loc[df_TweetFileArchiveClean.tweet_id == 740373189193256964, 'rating_numerator'] = '14'
df_TweetFileArchiveClean.loc[df_TweetFileArchiveClean.tweet_id == 716439118184652801, 'rating_numerator'] = '11'
df_TweetFileArchiveClean.loc[df_TweetFileArchiveClean.tweet_id == 682962037429899265, 'rating_numerator'] = '10'
df_TweetFileArchiveClean.loc[df_TweetFileArchiveClean.tweet_id == 666287406224695296, 'rating_numerator'] = '9'
df_TweetFileArchiveClean['rating_denominator'].mask(df_TweetFileArchiveClean.tweet_id.isin(tweet_id_list), '10',
                                                    inplace=True)
df_TweetFileArchiveClean['rating_denominator'] = df_TweetFileArchiveClean['rating_denominator'].fillna(0).astype(int)
df_TweetFileArchiveClean = df_TweetFileArchiveClean[df_TweetFileArchiveClean['rating_denominator'] == 10]

# Define
# 
#     When we only look at tweets with rating_denominator of 10, there are 12 tweets with rating_numerator >= 15
#     
# Code

df_TweetFileArchiveClean['rating_numerator'] = df_TweetFileArchiveClean['rating_numerator'].fillna(0).astype(int)
df_TweetFileArchiveClean = df_TweetFileArchiveClean[df_TweetFileArchiveClean['rating_numerator'] < 15]

# Define
# 
#     Invalid names like a, in, an should be chnaged to None
#     
# Code
df_TweetFileArchiveClean.name = df_TweetFileArchiveClean.name.str.replace(r'^[a-z]', '')

# # TEST
df_TweetFileArchiveClean.info()
df_TweetFileArchiveClean.source.value_counts()
df_TweetFileArchiveClean.name.value_counts()
df_TweetFileArchiveClean[df_TweetFileArchiveClean.tweet_id.isin(tweet_id_list)]
df_TweetFileArchiveClean[df_TweetFileArchiveClean['rating_denominator'] != 10]
df_TweetFileArchiveClean[df_TweetFileArchiveClean['rating_numerator'] >= 15]

# Define
# 
#     When all rating_denominators are the same, this column is no longer needed.
# 
# Code

df_TweetFileArchiveClean = df_TweetFileArchiveClean.drop(['rating_denominator'], axis=1)

# Define
# 
#     There are four columns for dog stages which can be accomodated in one
# 
# Code
df_TweetFileArchiveClean = pd.melt(df_TweetFileArchiveClean,
                                   id_vars=['tweet_id', 'timestamp', 'source', 'text', 'expanded_urls',
                                            'rating_numerator', 'name'],
                                   value_vars=['doggo', 'floofer', 'puppo', 'pupper'], var_name=['stage'],
                                   value_name='category')

df_TweetFileArchiveClean = df_TweetFileArchiveClean.drop('stage', axis=1)
df_TweetFileArchiveClean = df_TweetFileArchiveClean.drop_duplicates()

# Define
# 
#     No need of separate json data as this can be combined to one dataframe which holds archive data
#     
# Code

df_TweetFileArchiveClean = pd.merge(df_TweetFileArchiveClean, df_json, on='tweet_id')
df_TweetFileArchiveClean.info()

#save dataframe to  csv file
df_TweetFileArchiveClean.to_csv(tweetCleanFile,index=False)

#analyse
df_TweetFileArchiveClean.timestamp.min(), df_TweetFileArchiveClean.timestamp.max()
df_TweetFileArchiveClean.describe()

# plot
# %matplotlib inline
# df_TweetFileArchiveClean.rating_numerator.plot(kind = 'hist', bins = 15)
#
# plt.xlim(0, 15)
# plt.ylabel('Number of Tweets')
# plt.xlabel('Rating')
# plt.title('Distribution of Ratings')
# plt.show();




