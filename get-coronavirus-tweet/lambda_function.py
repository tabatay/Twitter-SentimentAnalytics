import urllib
from requests_oauthlib import OAuth1
import requests
import sys
import boto3
import hashlib

import settings

def search_tweets(CK, CKS, AT, ATS, word, count, range):
    
    api_head = 'https://api.twitter.com/1.1/search/tweets.json?lang=ja&q='
    
    # 文字列設定
    word += ' exclude:retweets' # RTは除く
    word = urllib.parse.quote_plus(word)
    
    # リクエスト
    url = api_head+word+"&count="+str(count)
    auth = OAuth1(CK, CKS, AT, ATS)
    response = requests.get(url, auth=auth)
    data = response.json()['statuses']
    
    # 2回目以降のリクエスト
    cnt = 0
    tweets = []
    while True:
        if len(data) == 0:
            break
        cnt += 1
        if cnt > range:
            break
        for tweet in data:
            tweets.append(tweet['text'])
            maxid = int(tweet["id"]) - 1
        print("test")
        url = api_head+word+"&count="+str(count)+"&max_id="+str(maxid)
        response = requests.get(url, auth=auth)
        try:
            data = response.json()['statuses']
        except KeyError: # リクエスト回数が上限に達した場合のデータのエラー処理
            print('上限エラー')
            break
    return tweets
    
def lambda_handler(event, context):
    
    [CK,CKS,AT,ATS] = [settings.CK,settings.CKS,settings.AT,settings.ATS]

    # 検索時のパラメーター
    word = 'コロナ 旅行' 
    # word = sys.argv[1] # 検索ワード
    print(word)
    count = 10 # 一回あたりの検索数(最大100/デフォルトは15)
    range = 5 # 検索回数の上限値(最大180/15分でリセット)
    
    # ツイート検索・テキストの抽出
    tweets = search_tweets(CK, CKS, AT, ATS, word, count, range)
    # 検索結果を表示
    # print("tweets:"+str(tweets))
    
    # kinesisにin
    stream_name="coronavirus-tweet-stream"
    kinesis = boto3.client("kinesis", region_name="ap-northeast-1")
    word_utf8 = word.encode('utf-8')
    
    # partition_keyをハッシュ化
    partition_key = hashlib.md5(word_utf8).hexdigest()
    
    
    tweets_preprocessed = []
    for tweet in tweets:
        tweet_newline_deleted = tweet.replace('\n','') # ツイート内の改行を削除
        tweet_newline_deleted += '\n' # ツイート末尾で改行
        tweets_preprocessed.append(tweet_newline_deleted)
        
    print("tweets_newline_deleted:"+str(tweets_preprocessed))
    
    for tweet in tweets_preprocessed:
        response = kinesis.put_record(
            Data=tweet,
            PartitionKey=partition_key,
            StreamName=stream_name
        )
    print("respose:"+str(response))
    
    #print(tweets[0])
    #kinesis.put_record(StreamName=stream_name,Data=tweets[0],PartitionKey=partition_key)
    
    # 確認
    '''
    response = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId='',
        ShardIteratorType='AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER'|'TRIM_HORIZON'|'LATEST'|'AT_TIMESTAMP',
        StartingSequenceNumber='string',
        Timestamp=datetime(2015, 1, 1)
    )
    shards = stream['StreamDescription']['Shards'][0]['ShardId']
    kinesis_iterator = kinesis.get_shard_iterator(stream_name,shards,'LATEST') 
 
    next_iterator = None
    while True:
        if next_iterator is None:
            next_iterator = kinesis_iterator['ShardIterator']
        else:
            next_iterator = responce['NextShardIterator']
     
        responce = None
        responce = connection.get_records(next_iterator,limit=1)
        print(responce['Records'])
    '''