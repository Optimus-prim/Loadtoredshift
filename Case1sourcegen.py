import pandas as pd
import random
import boto3
from faker import Faker
from datetime import datetime
import json
fake = Faker()
CATEGORIES = ["Electronics", "Fashion", "Home", "Appliances", "Books"]
SOURCES = ["Amazon", "eBay", "Walmart", "Target"]

def generate_sample_data(num_records=100):
    data = [{
            "order_id": i,
            "product_name": fake.word().capitalize() + " " + random.choice(CATEGORIES),
            "category": random.choice(CATEGORIES) if random.random() > 0.15 else "",
            "price": round(random.uniform(5, 1500), 2) if random.random() > 0.1 else None,
            "date": fake.date_this_decade().strftime("%d/%m/%Y") if random.random() > 0.1 else None,
            "source": random.choice(SOURCES)}
        for i in range(1, num_records + 1)
    ]
    return data

def write_csv_to_s3(data, bucket, s3_key):
    s3 = boto3.client('s3')
    csv_data = pd.DataFrame(data).to_csv(index=False)
    try:
        s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_data)
        print("Data successfully written to S3.")
    except Exception as e:
        print("Error writing data to S3:", e)

if __name__ == "__main__":
    sample_data = generate_sample_data(200)
    if sample_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"sample_sales_data_{timestamp}.csv"
        pd.DataFrame(sample_data).to_csv(csv_filename, index=False)
        print(f"Sample sales data saved as '{csv_filename}'")
        write_csv_to_s3(sample_data, 'case1bucket-e2e', f'Raw_data/Bronze/{csv_filename}')
    else:
        print("No data available to write.")