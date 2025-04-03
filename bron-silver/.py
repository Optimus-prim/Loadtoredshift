import boto3
import pandas as pd
import re
import json
from io import StringIO
import sys
from awsglue.utils import getResolvedOptions

def is_valid_email(email):
    # checks the email is valid or not by syntax
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(pattern, email) is not None

def transform_data(raw_data):
    try:
        df = pd.json_normalize(
            raw_data,
            record_path='items',
            meta=['_id',
                    ['customer', 'name'],
                    ['customer', 'email'],
                    'order_date',
                    'total_price'
                ],
            record_prefix='item_'
        ) # flatten the Json filte
        
        df = df[df['_id'].str.match(r'^ORD\d+$', na=False)] # orderid must be in ORDXXX format
        df = df[df['customer.email'].apply(is_valid_email)] # calls is valid email fn to check if its a vaild email or not
        df.rename(columns={
            '_id': 'order_id',
            'customer.name': 'customer_name',
            'customer.email': 'customer_email',
            'item_product_name': 'product_name',
            'item_quantity': 'quantity',
            'item_price': 'price'
        }, inplace=True) 
        
        df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime("%m/%d/%Y") #date conversion to MM/DD/YYYY

        final_df = df[['order_id', 'customer_name', 'customer_email', 'order_date', 'product_name', 'quantity', 'price']] #Dataframe columns order
        final_df = final_df.drop_duplicates() # Drop duplicate rows based on all columns
        return final_df
        
    except Exception as e:
        print(f"Error transforming data: {e}")
        return pd.DataFrame()

def read_json_from_s3(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    data_str = obj['Body'].read().decode('utf-8')
    return json.loads(data_str)

def write_csv_to_s3(df, bucket, key):
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def main():
    # Get job parameters from Glue job (provided in job configuration)
    args = getResolvedOptions(sys.argv, ['bucket', 'input_key', 'output_key'])
    
    bucket = args['bucket']
    input_key = args['input_key']
    output_key = args['output_key']

    # Read raw JSON data from S3.
    raw_data = read_json_from_s3(bucket, input_key)
    
    # Transform the data.
    transformed_df = transform_data(raw_data)
    if transformed_df.empty:
        print("Transformation resulted in empty data. Nothing to write.")
    else:
        # Write the transformed data as CSV to S3.
        write_csv_to_s3(transformed_df,bucket, output_key)
        print("Transformation complete and CSV written to S3.")

if __name__ == "__main__":
    main()


