# s3-dd-metric
Derive Metrics from S3 objects and Send to Datadog


### Dependencies
- Python3
- datadog python3 module (pip3 install datadog)
- boto3 python3 module (pip3 install boto3)
- fire python3 module (pip3 install fire)
- AWS S3 bucket Access(There are multiple ways to achieve this).
  1. Have AWS Access Key & Secret Key exported as system environment variables (AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY)
  2. Do an `aws configure` using cli (Input the access key and secret keys when asked)
  3. Use an IAM role tied up to the instance from where this script is executed (The IAM role should have s3 bucket access for getting objects)



### Examples

```
python3 metrics.py --bucketName=my-personal-bucket --target=Print
```

The above will print the transaction IDs found in today's and yesterdays directories in the bucket. Remember this will just print on the screen, without any metric formatting. This is helpful to actually see the transaction ids in standard output.

```
python3 metrics.py --bucketName=my-personal-bucket
```

The above will print the actual metrics that were calculated (ie: new transactions vs old transactions)

```
python3 metrics.py --bucketName=my-personal-bucket --date='2021-08-30'
```

The above will consider today's date as 30th August 2021 instead of the actual date.

```
python3 metrics.py --bucketName=my-personal-bucket --date='2021-08-30' --target=Datadog
```

The above will send the actual metrics to datadog
