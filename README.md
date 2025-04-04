

This document outlines the architecture, design, and implementation details of an end-to-end ETL pipeline built on AWS. I have designed this pipeline to ingest, transform, and load data into Amazon Redshift and then sends a notification on Slack.

### Pipeline Architecture:
[Local Hosted CSVs] ➔ [Bronze Layer] ➔ [Glue Transformation Job] ➔ [Silver Layer] ➔ [LambdaTrigger] ➔ [Glue Redshift Load Job] ➔[Amazon Redshift] ➔ [Slack Notification]

Step-by-Step Pipeline Design
1. Local Hosted CSVs (Data Source)
To simulate a realistic organizational data source, I created a simple Python script that generates CSV files locally.Each run produces a CSV file with a unique timestamp in its filename for better traceability.These CSV files are uploaded to an S3 bucket (Bronze layer) as part of the pipeline workflow.

2. Bronze Layer (Raw Data Storage)
Data is stored in its raw form without any transformations.Each newly generated CSV file is directly uploaded to the Bronze bucket.

3. AWS Glue Transformation Job
The Glue job reads data from the Bronze bucket, performs data cleaning and transformation logic, and writes the refined data to the Silver bucket.
I developed a Glue job that consolidates multiple CSV files from the Bronze layer into a single CSV file, which is then stored in the Silver bucket.
Performed chunk processing by passing 100 rows per chunk to simulate batch processing efficiently.

Key transformations performed:
Dropped duplicate records to improve data quality.
Filled null values in the price column using the mean of the respective category.
Formatted date fields in the YYYY/DD/MM format.
Added a new column to classify products as Expensive or Affordable.
Standardized the first word of each category by capitalizing it for consistency.

All transformation logic is written in Python within the Glue job, scheduled to run daily at 8 AM with Glue workflows.

To avoid redundancy or processing already processed files, I have logged the processed file names in a .txt file and stored it in S3 for reference.
4. Silver Layer (Cleaned Data Storage)
The transformed data is stored in the Silver bucket, serving as the staging area for Redshift ingestion.
An Event Notification is triggered when a new file is added to the Silver bucket. This notification invokes a Lambda function to proceed with Redshift loading.
5. AWS Lambda Function (Trigger Mechanism)
An S3 Event Notification triggers a Lambda function when new data is uploaded to the Silver bucket.
The Lambda function extracts metadata such as file path and timestamp.Invokes the second Glue job for Redshift loading.
6. AWS Glue Job (Redshift Load Job)
This Glue job efficiently loads data from the Silver bucket into Amazon Redshift.Leveraged the COPY command for efficient data loading.Before loading data, the Glue job includes a create if not exists logic to create the table with an expected schema.
Implemented incremental loading logic to avoid data duplication.
7. Amazon Redshift (Data Warehouse)
Data is stored in optimized Redshift tables by connecting the glue job with the redshift clusters, we can use the serverless, they can reduce the compute costs compared to general clusters. Copy Activity is widely used for scalability and writing a large amount of data. And my sales data after cleaning and transforming the data is Finally loaded into the redshift database and it looks like the below picture.


8. Slack Notification
Upon successful completion (or failure) of the Glue job, a Slack notification is triggered via AWS Lambda and CloudWatch rules. Notifications include pipeline status (Success/Failure) as mentioned in the rules.


I have used logger in the code for logging, but all the print statements in the pipeline code, were sent to cloudwatch groups, we can see all the logs there.

