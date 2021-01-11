import urllib
from requests_oauthlib import OAuth1
import requests
import sys
import boto3
import hashlib

import settings

kinesis = boto3.client('kinesis', region_name='ap-northeast-1')

def search_tweets(CK, CKS, AT, ATS, search_keyword, count, range):
    
    twitter_api_head = 'https://api.twitter.com/1.1/search/tweets.json?lang=ja&q='
    
    # 検索結果のうちリツイートを除外
    search_keyword += ' exclude:retweets'
    # URLエンコード
    search_keyword = urllib.parse.quote_plus(search_keyword) 
    
    # リクエストURL
    twitter_url = twitter_api_head+search_keyword+'&count='+str(count)+'&tweet_mode=extended'
    
    # OAuth1.0での認証
    auth = OAuth1(CK, CKS, AT, ATS)
    
    # 検索数制限越えの確認
    try:
        response = requests.get(twitter_url, auth=auth)
        search_data = response.json()['statuses']
    except Exception as e:
        print(e)
    
    print('検索結果:'+str(search_data))
    
    # 2回目以降のリクエスト
    count = 0
    tweets = []
    while True:
        # 検索結果が存在するか
        if len(search_data) == 0:
            break
        
        count += 1
        
        # 指定回以上リクエストしたか
        if count > range:
            break
        
        print('検索結果:'+str(search_data))
        
        for tweet in search_data:
            tweets.append(tweet['full_text'])
            max_id = int(tweet['id']) - 1
        
        twitter_url = twitter_api_head+search_keyword+'&count='+str(count)+'&max_id='+str(max_id)+'&tweet_mode=extended'
        
        # 検索数制限越えの確認
        try:
            response = requests.get(twitter_url, auth=auth)
            search_data = response.json()['statuses']
        except Exception as e:
            print(e)
            break
    
    return tweets
    
def lambda_handler(event, context):
    
    [CK,CKS,AT,ATS] = [settings.CK,settings.CKS,settings.AT,settings.ATS]
    
    search_keyword = settings.search_keyword
    print('検索キーワード:'+str(search_keyword))

    # 1回あたりのツイート検索件数(最大100, デフォルトは15件)
    count = settings.count 
    # 検索回数の上限値(最大180, 15分でリセット)
    range = settings.range 
    
    # ツイート検索・テキストの抽出
    tweets = search_tweets(CK, CKS, AT, ATS, search_keyword, count, range)
    
    tweets_preprocessed = []
    for tweet in tweets:
        # ツイート内に存在する改行を削除
        tweet_newline_deleted = tweet.replace('\n','') 
        # ツイート末尾で改行
        tweet_newline_deleted += '\n' 
        
        tweets_preprocessed.append(tweet_newline_deleted)
        
    print('tweets_preprocessed:'+str(tweets_preprocessed))
    
    # Kinesis設定
    stream_name = settings.stream_name
    search_keyword_utf8 = search_keyword.encode('utf-8')
    
    # partition_keyは検索キーワードでハッシュ化したものを指定
    partition_key = hashlib.md5(search_keyword_utf8).hexdigest()
    
    # 1件ずつKinesisDataStreamにput
    for tweet in tweets_preprocessed:
        response = kinesis.put_record(
            Data=tweet,
            PartitionKey=partition_key,
            StreamName=stream_name
        )

    print('kinesis.put_record respose:'+str(response))