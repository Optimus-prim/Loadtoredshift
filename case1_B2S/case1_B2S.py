import boto3
import pandas as pd
import logging
from io import BytesIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Extract Glue arguments
args = getResolvedOptions(sys.argv, ['input_bucket', 'input_prefix', 'output_bucket', 'output_folder', 'log_prefix'])

# Initialize boto3 client
s3 = boto3.client('s3')

# Read the processed log file
def read_processed_log(bucket, log_prefix):
    try:
        logger.info("Reading processed log file...")
        obj = s3.get_object(Bucket=bucket, Key=log_prefix)
        processed_files = set(obj['Body'].read().decode('utf-8').strip().split('\n'))
        logger.info(f"Processed log read successfully with {len(processed_files)} entries.")
        return processed_files
    except s3.exceptions.NoSuchKey:
        logger.warning("Processed log file not found. Assuming no files processed yet.")
        return set()

# Write processed files to log
def write_processed_log(bucket, log_prefix, processed_files):
    logger.info(f"Updating processed log with {len(processed_files)} files.")
    s3.put_object(Bucket=bucket, Key=log_prefix, Body='\n'.join(processed_files))
    logger.info("Processed log updated successfully.")

# Read new CSV files from Bronze Layer in chunks
def read_csv_from_s3(bucket, prefix, processed_files, chunk_size=100):
    csv_chunks = []
    new_processed_files = set()

    logger.info("Fetching new data files from Bronze Layer...")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.csv') and key not in processed_files:
            logger.info(f"Reading new file: {key}")
            data = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
            for chunk in pd.read_csv(BytesIO(data.encode()), chunksize=chunk_size):
                csv_chunks.append(chunk)
            new_processed_files.add(key)

    logger.info(f"Total new files to process: {len(new_processed_files)}")
    return csv_chunks, new_processed_files

# Data Cleaning
def clean_data(df):
    logger.info("Starting data cleaning...")
    try:
        df.drop_duplicates(inplace=True)
        df['price'].fillna(df.groupby('category')['price'].transform('mean'), inplace=True)
        df['category'] = df['category'].apply(lambda x: x.split()[0] if pd.notna(x) else x)
        df['date'] = pd.to_datetime(df['date'], format="%d/%m/%Y").dt.strftime("%Y/%m/%d")
        df['price_category'] = df['price'].apply(lambda x: 'Expensive' if x > 100 else 'Affordable')
        logger.info(f"Data cleaning completed successfully. {len(df)} records cleaned.")
        return df
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}", exc_info=True)
        raise

# Write data to Silver Layer as CSV
def write_csv_to_s3(df, bucket, prefix):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_key = f"{prefix}/sales_data_{timestamp}.csv"

    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    try:
        s3.put_object(Bucket=bucket, Key=file_key, Body=buffer.getvalue())
        logger.info(f"Data successfully written to {file_key}")
    except Exception as e:
        logger.error(f"Error writing CSV to Silver Layer: {e}", exc_info=True)
        raise

# Main Execution
def main():
    logger.info("Starting Bronze to Silver ETL Pipeline...")
    
    processed_files = read_processed_log(args['output_bucket'], args['log_prefix'])

    bronze_data_chunks, new_processed_files = read_csv_from_s3(
        args['input_bucket'], args['input_prefix'], processed_files, chunk_size=100
    )

    if not bronze_data_chunks:
        logger.info("No new data found. Nothing to write.")
    else:
        for chunk in bronze_data_chunks:
            cleaned_data = clean_data(chunk)
            write_csv_to_s3(cleaned_data, args['output_bucket'], args['output_folder'])

        # Update the processed log file
        write_processed_log(args['output_bucket'], args['log_prefix'], processed_files.union(new_processed_files))
        logger.info("Data consolidation and write to Silver Layer completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Pipeline failed due to an unexpected error: {e}", exc_info=True)

