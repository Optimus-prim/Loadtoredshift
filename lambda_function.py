# import json
# import boto3

# def lambda_handler(event, context):
#     glue = boto3.client('glue')
    
#     # Extract S3 event details
#     record = event['Records'][0]
#     bucket = record['s3']['bucket']['name']
#     key = record['s3']['object']['key']
    
#     # Define your Glue job name
#     job_name = "case1_S2R"  # Replace with your actual Glue job name
    
#     # Pass job parameters if needed. For example, you can pass the S3 path of the loaded file.
#     job_args = {
#         '--input_bucket': bucket,
#         '--input_key': key
#     }
#     print(bucket)
#     print(key)
#     try:
#         response = glue.start_job_run(
#             JobName=job_name,
#             Arguments=job_args
#         )
#         return {
#             'statusCode': 200,
#             'body': json.dumps(f"Glue job started successfully with JobRunId: {response['JobRunId']}")
#         }
#     except Exception as e:
#         return {
#             'statusCode': 500,
#             'body': json.dumps(f"Error starting Glue job: {str(e)}")
#         }




import json
import boto3
import logging

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    glue = boto3.client('glue')
    
    # Extract S3 event details
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        logger.info(f"Received event for bucket: {bucket}, key: {key}")
    except (KeyError, IndexError) as e:
        logger.error("Error extracting S3 event details", exc_info=True)
        return {
            'statusCode': 400,
            'body': json.dumps("Invalid event structure.")
        }
    
    # Define Glue job details
    job_name = "case1_S2R"  # Replace with your actual Glue job name
    job_args = {
        '--input_bucket': bucket,
        '--input_key': key
    }
    
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=job_args
        )
        logger.info(f"Glue job '{job_name}' started successfully with JobRunId: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue job started successfully with JobRunId: {response['JobRunId']}")
        }
    except Exception as e:
        logger.error(f"Error starting Glue job '{job_name}': {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error starting Glue job: {str(e)}")
        }

