import boto3

def lambda_handler(event, context):
    
    comprehend = boto3.client("comprehend")
    s3_client = boto3.client('s3')
    
    s3_event_uri = event['Records'][0]['s3']['object']['key']
    receive_bucket_name = 'cn2020-s3-raw'
    
    print(s3_event_uri)
    
    response = s3_client.get_object(Bucket=receive_bucket_name, Key=s3_event_uri)
    lines = response['Body'].read().decode('utf-8').splitlines()
    
    print(response['Body'].read().splitlines())
    print(lines)
    
    result_str = ''
    
    for text in lines:
        comprehend_result = comprehend.detect_sentiment(Text=text, LanguageCode="ja")
        print(comprehend_result)
        
        result_str += str(comprehend_result) + '¥r¥n'
    
    s3_put_uri = 'comprehend-executed-coronavirus-result/'
    
    put_bucket_name = 'cn2020-s3-lake'
    
    put_object = s3_client.put_object(Body=result_str,Bucket=put_bucket_name, Key=s3_put_uri)
    
    # start_response = comprehend.start_dominant_language_detection_job(
    #     InputDataConfig={
    #         'S3Uri': 's3://cn2020-s3-raw/'+str(s3_uri),
    #         'InputFormat': 'ONE_DOC_PER_FILE'
    #     },
        
    #     OutputDataConfig={
    #         'S3Uri': 's3://cn2020-s3-lake/comprehend-executed-coronavirus-result/'
    #     },
        
    #     DataAccessRoleArn='arn:aws:iam::340759393322:role/tabata-role',
    #     JobName='coronavirus-tweets'
    # )
    
    return 0