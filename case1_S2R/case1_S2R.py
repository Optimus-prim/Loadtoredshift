import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
import json

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Extract Arguments
args = getResolvedOptions(
    sys.argv,
    [
        'input_bucket',
        'input_key',
        'REDSHIFT_CLUSTER_ID',
        'REDSHIFT_DATABASE',
        'REDSHIFT_DB_USER',
        'IAM_ROLE',
        'TABLE_NAME',
        'REGION'
    ]
)

input_bucket = args['input_bucket']
input_key = args['input_key']
REDSHIFT_CLUSTER_ID = args['REDSHIFT_CLUSTER_ID']
REDSHIFT_DATABASE = args['REDSHIFT_DATABASE']
REDSHIFT_DB_USER = args['REDSHIFT_DB_USER']
IAM_ROLE = args['IAM_ROLE']
TABLE_NAME = args['TABLE_NAME']
REGION = args['REGION']

def main():
    # Initialize Redshift Data API client
    redshift_data = boto3.client('redshift-data', region_name=REGION)

    # OPTIONAL: Create table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        order_id VARCHAR(50),
        product_name VARCHAR(100),
        category VARCHAR(50),
        price FLOAT,
        order_date DATE,
        source VARCHAR(20),
        price_category VARCHAR(20)
    );
    """

    try:
        logger.info(f"Creating table if not exists: {TABLE_NAME}")
        resp_create = redshift_data.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DATABASE,
            DbUser=REDSHIFT_DB_USER,
            Sql=create_table_sql
        )
        create_statement_id = resp_create['Id']
        logger.info(f"Create Table Statement ID: {create_statement_id}")
    except Exception as e:
        logger.error(f"Error creating table {TABLE_NAME}: {e}", exc_info=True)
        sys.exit(1)

    # S3 path
    s3_path = f"s3://{input_bucket}/{input_key}"

    # COPY command to load CSV from S3
    copy_sql = f"""
    COPY {TABLE_NAME}
    FROM '{s3_path}'
    IAM_ROLE '{IAM_ROLE}'
    FORMAT AS CSV
    DELIMITER ','
    IGNOREHEADER 1
    REGION '{REGION}'
    DATEFORMAT 'YYYY/MM/DD';
    """

    try:
        logger.info(f"Running COPY command for table {TABLE_NAME}...")
        resp_copy = redshift_data.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DATABASE,
            DbUser=REDSHIFT_DB_USER,
            Sql=copy_sql
        )
        copy_statement_id = resp_copy['Id']
        logger.info(f"COPY Statement ID: {copy_statement_id}")
        logger.info("Data load request successfully submitted to Redshift.")
    except Exception as e:
        logger.error(f"Error loading data into {TABLE_NAME}: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Pipeline failed due to unexpected error: {e}", exc_info=True)
        sys.exit(1)
