import re
import sys
import pytz
import time
import json
import boto3
import email
import pandas as pd
from io import StringIO
import redshift_connector
from datetime import datetime 
from awsglue.utils import getResolvedOptions

# Input Arguments from the Environment of glue
args = getResolvedOptions(sys.argv, ['bucket_name','prefix'])


# Changing TimeZone while Uploading Data to Redshift or S3
timezone = pytz.timezone('Asia/Kolkata')
bucketName=args['bucket_name']
prefixWithFileName=args['prefix']
region_name = 'eu-west-1'
aws_secret_access_key = ''
aws_access_key_id = ''

environ = 'dev'
regex='\\<(.*?)\\>'
prefix=f'processed-email/{environ}/'
fileName='decodedEmail'



def read_email_from_s3(bucket_name, file_name):
    s3_resource = boto3.resource('s3', 
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key, 
            region_name=region_name
    )
    obj = s3_resource.Object(bucket_name, file_name)
    body = obj.get()['Body'].read().decode('utf-8')
    return body

    
def upload_csv_in_s3(attachment_csv, bucket_name, prefix, file_name):
    s3_resource = boto3.client('s3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key, 
        region_name=region_name
    )
    date_val= '_' + datetime.now(timezone).strftime('%Y-%m-%d')
    log_table_id = str(int(time.time()))
    # MetaData info that is passed with fileName 
    metadata = {
        'log_table_id': log_table_id,
        'data_transmission_type': 'Email',
        'is_latest_file': 'Yes'
    } 
    s3_resource.upload_file(
            attachment_csv, 
            bucket_name, 
            prefix+'/' + file_name + date_val + '-' + log_table_id + '.csv', 
            ExtraArgs={"Metadata": metadata}
    )





txt=read_email_from_s3(bucketName, prefixWithFileName)
msg=email.message_from_string(txt)
from_add=msg['from']
subject=msg['subject'].split(' ')[1].lower().strip()
attachment=msg.get_payload()[1]
from_add=re.findall(regex,from_add)[0]
open('/tmp/attachment.csv','wb').write(attachment.get_payload(decode=True))
upload_csv_in_s3('/tmp/attachment.csv', bucketName, prefix=prefix, file_name=fileName)