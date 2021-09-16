import boto3
import fire
import datetime
import json

s3 = boto3.client('s3')


def get_transaction_ids(bucket_name, today):
    try:
        yesterday = datetime.date.fromisoformat(today)-datetime.timedelta(1)
        yesterday = yesterday.strftime('%Y-%m-%d')
        transactions = {}
        todays_files = s3.list_objects(Bucket = bucket_name, Prefix=today)
        todays_transactions = []
        if todays_files.get('Contents') != None:
            for file in todays_files.get('Contents'):
                content = s3.get_object(Bucket=bucket_name, Key=file.get('Key'))
                json_data = json.loads(content['Body'].read().decode("utf-8"))
                transaction_id = json_data['transaction_id'] 
                todays_transactions.append(transaction_id)
        yesterdays_files = s3.list_objects(Bucket = bucket_name, Prefix=yesterday)
        yesterdays_transactions = []
        if yesterdays_files.get('Contents') != None:
            for file in yesterdays_files.get('Contents'):
                content = s3.get_object(Bucket=bucket_name, Key=file.get('Key'))
                json_data = json.loads(content['Body'].read().decode("utf-8"))
                transaction_id = json_data['transaction_id']
                yesterdays_transactions.append(transaction_id)
    except Exception as e:
        print ("Error happened while Fetching TansactionIds: "+e)
    return {'yesterdays_transactions': yesterdays_transactions, 'todays_transactions': yesterdays_transactions}


def main(bucketName, date=datetime.datetime.today().strftime('%Y-%m-%d'), target="Print"):
    try:
        if target == 'Print':
            print(get_transaction_ids(bucketName, date))
        if target == 'Datadog':
            pass
    except Exception as e:
        print ("Something Went Wrong: "+e)

if __name__ == '__main__':
    fire.Fire(main)
