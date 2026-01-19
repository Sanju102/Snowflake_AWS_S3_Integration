-- 1. Create a db called ecom 
CREATE OR REPLACE DATABASE ECOM;
-- 2. Use database ECOM
USE ECOM;
-- 3. Create schema orders
CREATE OR REPLACE SCHEMA ORDERS;

-- 4. Use the Schema
USE SCHEMA ORDERS;

-- 5. Create the Sales table
CREATE OR REPLACE TABLE Sales (
    OrderID INT NOT NULL PRIMARY KEY, 
    CustomerID INT NOT NULL, 
    ProductID VARCHAR(10) NOT NULL,
    Quantity INT NOT NULL
);

-- 6. Create integration object for external stage 
CREATE OR REPLACE STORAGE INTEGRATION S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED=TRUE 
    storage_aws_role_arn='<Storage AWS role ARN>'
    storage_allowed_locations=('<S3 bucket location>');

-- 7. Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION S3_INT;

CREATE OR REPLACE FILE FORMAT ECOM.ORDERS.csv_format
    type=csv 
    field_delimiter=','
    skip_header=1
    null_if=('NULL','null')
    empty_field_as_null= true;

CREATE OR REPLACE STAGE ECOM.ORDERS.EXT_STAGE
    URL='<S3 bucket location>'
    STORAGE_INTEGRATION=S3_INT
    FILE_FORMAT=ECOM.ORDERS.csv_format;

-- 8. create pipe to automate data ingestion from s3 to snowflake
CREATE OR REPLACE PIPE ECOM.ORDERS.MYPIPE AUTO_INGEST=TRUE AS
COPY INTO SALES
FROM @ECOM.ORDERS.EXT_STAGE
ON_ERROR=CONTINUE;

SHOW PIPES;
SELECT * FROM ECOM.ORDERS.SALES;
