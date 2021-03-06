import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import settings

JST = timezone(timedelta(hours=+9), 'JST')

comprehend = boto3.client('comprehend')
s3_client = boto3.client('s3')

def detect_sentiment(text,language_code, i):
    
    # comprehendの機能でツイートのPositive,Negative,Neutral,Mixed度を分析
    response = comprehend.detect_sentiment(Text=text, LanguageCode=language_code)
    
    print('感情分析結果 ' + str(i) + '行目' + str(response))
    
    tmp_detect_sentiment_str = ''
    
    # Positive,Negative,Neutral,Mixed度は小数第4位まで
    tmp_detect_sentiment_str += text + ','
    tmp_detect_sentiment_str += str(response['Sentiment']) + ','
    tmp_detect_sentiment_str += str(Decimal(str(response['SentimentScore']['Positive'])).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)) + ','
    tmp_detect_sentiment_str += str(Decimal(str(response['SentimentScore']['Negative'])).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)) + ','
    tmp_detect_sentiment_str += str(Decimal(str(response['SentimentScore']['Neutral'])).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)) + ','
    tmp_detect_sentiment_str += str(Decimal(str(response['SentimentScore']['Mixed'])).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP))
    
    tmp_detect_sentiment_str += '\n'
    
    return tmp_detect_sentiment_str


def detect_key_phrases(text, language_code, i):
    
    # comprehendの機能でツイート内に存在するキーフレーズを抽出
    response = comprehend.detect_key_phrases(Text=text, LanguageCode=language_code)
    
    print('キーフレーズ抽出結果 ' + str(i) + '行目' + str(response))

    tmp_detect_sentiment_str = ''
    
    for element in response['KeyPhrases']:
        tmp_detect_sentiment_str += element['Text'] + '\n'

    return tmp_detect_sentiment_str


def put_s3_object(s3_put_folder, s3_put_filename, result):

    s3_put_filename += str(datetime.now(JST).strftime('%Y%m%d%H%M%S')) + '.csv'
    s3_put_uri = s3_put_folder + s3_put_filename
    
    # 結果を格納
    s3_put_object = s3_client.put_object(Body=result, Bucket=settings.put_bucket_name, Key=s3_put_uri)
    
    print(s3_put_object)
    
    return 0
    

def lambda_handler(event, context):
    
    s3_event_uri = event['Records'][0]['s3']['object']['key']
    print('格納されたS3ファイルのURI:s3://' + settings.receive_bucket_name + str(s3_event_uri))
    
    # ファイルの中身を行ごとに読込
    s3_object = s3_client.get_object(Bucket=settings.receive_bucket_name, Key=s3_event_uri)
    s3_object_body_lines = s3_object['Body'].read().decode('utf-8').splitlines()

    # ファイルのヘッダ
    result_str = 'Sentiment,Positive,Negative,Neutral,Mixed\n'
    
    # キーフレーズ抽出ファイルのヘッダ
    key_phrases_result_str = 'KeyPhrases\n'
    
    # 分析結果をカンマ区切りで行ごとに書き込むための加工
    i = 1
    for text in s3_object_body_lines:
        
        detect_sentiment_result = detect_sentiment(text, 'ja', i)
        result_str += detect_sentiment_result
        
        # キーフレーズ抽出
        key_phrases_result = detect_key_phrases(text, 'ja', i)
        key_phrases_result_str += key_phrases_result
    
        # 行数の計算
        i += 1
    
    # 結果格納先パスの宣言
    s3_detect_sentiment_put_folder = settings.s3_detect_sentiment_put_folder
    s3_detect_sentiment_put_filename = settings.s3_detect_sentiment_put_filename
    put_s3_object(s3_detect_sentiment_put_folder, s3_detect_sentiment_put_filename, result_str)
    
    s3_detect_key_phrases_put_folder = settings.s3_detect_key_phrases_put_folder
    s3_detect_key_phrases_put_filename = settings.s3_detect_key_phrases_put_filename
    put_s3_object(s3_detect_key_phrases_put_folder, s3_detect_key_phrases_put_filename, key_phrases_result_str)
    
    return 0