

This document outlines the architecture, design, and implementation details of an end-to-end ETL
pipeline built on AWS. I have designed this pipeline to ingest, transform, and load data into Amazon
Redshift.

#### Pipeline Architecture:
[Local Hosted CSVs] ➔ [Bronze Layer] ➔ [Glue Transformation Job] ➔ [Silver Layer] ➔ [LambdaTrigger] ➔ [Glue Redshift Load Job] ➔[Amazon Redshift] ➔ [Slack Notification]
