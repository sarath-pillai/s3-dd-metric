import boto3
import fire
import os
import datetime
import json
from datadog import initialize, api


s3 = boto3.client('s3')


def get_transaction_ids(bucket_name, today):
    """
    This function gets all objects from the required dates and then gets transactionIds from each of those.
    The function then returns just the list of transactions found with their values. It does not return the count or any metrics. 
    This way its easier to view the transaction Ids as well if required. 
    """
    try:
        yesterday = datetime.date.fromisoformat(today)-datetime.timedelta(1)
        yesterday = yesterday.strftime('%Y-%m-%d')
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
    return {'yesterdays_transactions': yesterdays_transactions, 'todays_transactions': todays_transactions}


def transaction_metrics(bucket_name, today):
    """
    This function does the actual math and calculates if there any new transaction ids or not, and if there is how many of them are there. 
    It also returns the lost transactions.
    """
    try:
        transaction_data = get_transaction_ids(bucket_name, today)
        yesterday = transaction_data['yesterdays_transactions']
        today = transaction_data['todays_transactions']
        t = round(datetime.datetime.now().timestamp())
        new_transactions = len(list(set(today) - set(yesterday)))
        lost_transactions = len(list(set(yesterday) - set(today)))
        new_transactions_metrics = { "metric": "business.a_process.transaction_new", "value": new_transactions, "timestamp": t }
        lost_transactions_metrics = { "metric": "business.a_process.transaction_lost", "value": lost_transactions, "timestamp": t }
        return (new_transactions_metrics, lost_transactions_metrics)
    except Exception as e:
        print ("Error happened while Generating Metrics: "+e)
        

def send_to_datadog(metrics):
    """
    This function does the job of send a given set of metrics to datadog. 
    The function accept a list of maps containing the metrics.
    """
    try:
        if "DATADOG_API_KEY" in os.environ:
            initialize()
            for m in metrics:
                host = None
                api.Metric.send(
                    metric=m['metric'],
                    points=m['value'],
                    tags=[],
                    host=host
                )
    except Exception as e:
        print ("Error happened while Sending Data to DataDog: "+e) 

def main(bucketName, date=datetime.datetime.today().strftime('%Y-%m-%d'), target="Metric"):
    """
    This is a simple Python script that can grab objects from a given S3 bucket and optionally send metrics related to transactions to datahub.
    """
    try:
        if target == 'Print':
            print(get_transaction_ids(bucketName, date))
        if target == 'Datadog':
            metrics = []
            new, lost = transaction_metrics(bucketName, date)
            metrics.append(new)
            metrics.append(lost)
            send_to_datadog(metrics)
        if target == 'Metric':
            new, lost  = transaction_metrics(bucketName, date)
            print (new, lost, sep='\n')
    except Exception as e:
        print ("Something Went Wrong: "+e)

if __name__ == '__main__':
    fire.Fire(main)
