-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Creating File Format 
CREATE OR REPLACE FILE FORMAT insurance_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE;

-- Creating Storage Integration for Snowflake to S3 connection.
CREATE OR REPLACE STORAGE INTEGRATION my_integration
STORAGE_PROVIDER = 'S3'
TYPE = EXTERNAL_STAGE
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::724268665020:role/INSURANCE_DATA_INSIGHTS_ROLE'
STORAGE_ALLOWED_LOCATIONS = ('s3://insurancedatainsigths/insurance_csv_files/');
DESCRIBE INTEGRATION my_integration;

-- Crearting an external stage for importing data.
CREATE OR REPLACE STAGE insurance_external_stage
FILE_FORMAT = insurance_csv_format
STORAGE_INTEGRATION  = my_integration
URL = 's3://insurancedatainsigths/insurance_csv_files/';

LIST @insurance_external_stage;
------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_POLICY (
    POLICY_ID STRING,
    CUSTOMER_ID STRING,
    POLICY_TYPE STRING,
    PRODUCT_NAME STRING,
    PREMIUM_AMOUNT STRING,
    SUM_ASSURED STRING,
    START_DATE STRING,
    END_DATE STRING,
    POLICY_STATUS STRING,
    UNDERWRITER_ID STRING,
    AGENT_ID STRING,
    LOADED_AT TIMESTAMP,
    SOURCE_FILE STRING
);

CREATE OR REPLACE TABLE RAW_CLAIMS(
    CLAIM_ID STRING,
    POLICY_ID STRING,
    CLAIM_DATE STRING,
    REPORTED_DATE STRING,
    CLAIM_AMOUNT STRING,
    APPROVED_AMOUNT STRING,
    CLAIM_REASON STRING,
    CLAIM_STATUS STRING,
    SURVEYOR_ID STRING,
    IS_FLAGGED STRING,
    LOADED_AT TIMESTAMP,
    SOURCE_FILE STRING
);

CREATE OR REPLACE TABLE RAW_CUSTOMER (
    CUSTOMER_ID STRING,
    NAME STRING,
    AGE STRING,
    GENDER STRING,
    EMAIL STRING,
    PHONE STRING,
    ADDRESS STRING,
    STATE STRING,
    INCOME_GROUP STRING,
    OCCUPATION STRING,
    RISK_SCORE STRING,
    JOIN_DATE STRING,
    LOADED_AT TIMESTAMP,
    SOURCE_FILE STRING
);

-- Copy Into command from stage to respective table.
COPY INTO RAW_CUSTOMER
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, 
        $7, $8, $9, $10, $11, $12,
        CURRENT_TIMESTAMP(),        -- LOADED_AT
        METADATA$FILENAME           -- SOURCE_FILE
    FROM @insurance_external_stage/customer_master_uncleaned.csv
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO RAW_CLAIMS
FROM (
    SELECT 
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10,
        CURRENT_TIMESTAMP(),        -- LOADED_AT
        METADATA$FILENAME           -- SOURCE_FILE
    FROM @insurance_external_stage/claims_master_uncleaned.csv
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
COPY INTO RAW_POLICY
FROM (
    SELECT 
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10, $11,
        CURRENT_TIMESTAMP(),        -- LOADED_AT
        METADATA$FILENAME           -- SOURCE_FILE
    FROM @insurance_external_stage/policy_master_dirty_dates.csv
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SELECT * FROM RAW_POLICY;
SELECT * FROM RAW_CUSTOMER;
SELECT * FROM RAW_CLAIMS;
