USE DATABASE ABCD_DB;
USE SCHEMA VALIDATED_ABC_SCHEMA;
------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE VALIDATED_AGENTS (
    agent_id         STRING PRIMARY KEY,
    agent_name       STRING,
    region           STRING,
    email            STRING,
    phone            STRING,
    license_status   STRING,
    joining_date     DATE,
    branch_code      STRING,
    
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at       TIMESTAMP
);

CREATE OR REPLACE TABLE VALIDATED_RISK (
    risk_id              STRING PRIMARY KEY,
    policy_id            STRING,
    risk_category        STRING,
    risk_factor          STRING,
    risk_score           NUMBER,
    last_assessed_date   DATE,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at           TIMESTAMP
);
-----------------------------------------------------------------------------------------------------------------------------------------


MERGE INTO VALIDATED_AGENTS AS target
USING (
    SELECT
        UPPER(TRIM(RAW:agent_id::STRING))         AS agent_id,
        INITCAP(TRIM(RAW:agent_name::STRING))     AS agent_name,
        INITCAP(TRIM(RAW:region::STRING))         AS region,
        LOWER(TRIM(RAW:email::STRING))            AS email,
        TRIM(RAW:phone::STRING)                   AS phone,
        UPPER(TRIM(RAW:license_status::STRING))   AS license_status,
        TRY_TO_DATE(RAW:joining_date::STRING)     AS joining_date,
        UPPER(TRIM(RAW:branch_code::STRING))      AS branch_code
    FROM RAW_ABC_SCHEMA.RAW_AGENTS
    WHERE RAW:agent_id IS NOT NULL

    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY RAW:agent_id::STRING
            ORDER BY TRY_TO_DATE(RAW:joining_date::STRING) DESC
        ) = 1
) AS source

ON target.agent_id = source.agent_id

WHEN MATCHED THEN UPDATE SET
    target.agent_name      = source.agent_name,
    target.region          = source.region,
    target.email           = source.email,
    target.phone           = source.phone,
    target.license_status  = source.license_status,
    target.joining_date    = source.joining_date,
    target.branch_code     = source.branch_code,
    target.updated_at      = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    agent_id, agent_name, region, email,
    phone, license_status, joining_date, branch_code
)
VALUES (
    source.agent_id, source.agent_name, source.region,
    source.email, source.phone, source.license_status,
    source.joining_date, source.branch_code
);
------------------------------------------------------------------------------------------------------------------------------------------

MERGE INTO VALIDATED_RISK AS target
USING (
    SELECT
        UPPER(TRIM(RAW:risk_id::STRING))          AS risk_id,
        UPPER(TRIM(RAW:policy_id::STRING))        AS policy_id,
        INITCAP(TRIM(RAW:risk_category::STRING))  AS risk_category,
        INITCAP(TRIM(RAW:risk_factor::STRING))    AS risk_factor,
        TRY_TO_NUMBER(RAW:risk_score::STRING)     AS risk_score,
        TRY_TO_DATE(RAW:last_assessed_date::STRING) AS last_assessed_date
    FROM RAW_ABC_SCHEMA.RAW_RISK
    WHERE RAW:risk_id IS NOT NULL

    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY RAW:risk_id::STRING
            ORDER BY TRY_TO_DATE(RAW:last_assessed_date::STRING) DESC
        ) = 1
) AS source

ON target.risk_id = source.risk_id

WHEN MATCHED THEN UPDATE SET
    target.policy_id           = source.policy_id,
    target.risk_category       = source.risk_category,
    target.risk_factor         = source.risk_factor,
    target.risk_score          = source.risk_score,
    target.last_assessed_date  = source.last_assessed_date,
    target.updated_at          = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    risk_id, policy_id, risk_category,
    risk_factor, risk_score, last_assessed_date
)
VALUES (
    source.risk_id, source.policy_id,
    source.risk_category, source.risk_factor,
    source.risk_score, source.last_assessed_date
);
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM VALIDATED_AGENTS;
SELECT * FROM VALIDATED_RISK;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

-------------- INCREMENTAL UPDATES USING TASKS AND STREAM

----STREAM
CREATE OR REPLACE STREAM CREATING_STREAM_ON_BRONZE_FOR_INC_AGENTS
ON TABLE RAW_AGENTS
APPEND_ONLY = TRUE  
COMMENT = 'Captures new policy rows from COPY INTO';

CREATE OR REPLACE STREAM CREATING_STREAM_ON_BRONZE_FOR_INC_RISK
ON TABLE RAW_RISK
APPEND_ONLY = TRUE  
COMMENT = 'Captures new policy rows from COPY INTO';
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT
    SYSTEM$STREAM_HAS_DATA('CREATING_STREAM_ON_BRONZE_FOR_INC_AGENTS') AS bronze_agents_has_data,
    SYSTEM$STREAM_HAS_DATA('CREATING_STREAM_ON_BRONZE_FOR_INC_RISK')  AS bronze_risk_has_data;

----TASKS
CREATE OR REPLACE TASK TASK_LOAD_BRONZE_TO_SILVER_FOR_AGENTS
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA('CREATING_STREAM_ON_BRONZE_FOR_INC_AGENTS')
AS
MERGE INTO VALIDATED_AGENTS AS target
USING (
    SELECT
        UPPER(TRIM(RAW:agent_id::STRING))         AS agent_id,
        INITCAP(TRIM(RAW:agent_name::STRING))     AS agent_name,
        INITCAP(TRIM(RAW:region::STRING))         AS region,
        LOWER(TRIM(RAW:email::STRING))            AS email,
        TRIM(RAW:phone::STRING)                   AS phone,
        UPPER(TRIM(RAW:license_status::STRING))   AS license_status,
        TRY_TO_DATE(RAW:joining_date::STRING)     AS joining_date,
        UPPER(TRIM(RAW:branch_code::STRING))      AS branch_code
    FROM RAW_ABC_SCHEMA.RAW_AGENTS
    WHERE RAW:agent_id IS NOT NULL

    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY RAW:agent_id::STRING
            ORDER BY TRY_TO_DATE(RAW:joining_date::STRING) DESC
        ) = 1
) AS source

ON target.agent_id = source.agent_id

WHEN MATCHED THEN UPDATE SET
    target.agent_name      = source.agent_name,
    target.region          = source.region,
    target.email           = source.email,
    target.phone           = source.phone,
    target.license_status  = source.license_status,
    target.joining_date    = source.joining_date,
    target.branch_code     = source.branch_code,
    target.updated_at      = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    agent_id, agent_name, region, email,
    phone, license_status, joining_date, branch_code
)
VALUES (
    source.agent_id, source.agent_name, source.region,
    source.email, source.phone, source.license_status,
    source.joining_date, source.branch_code
);
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
    

CREATE OR REPLACE TASK TASK_LOAD_BRONZE_TO_SILVER_FOR_RISK
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA('CREATING_STREAM_ON_BRONZE_FOR_INC_AGENTS')
AS
MERGE INTO VALIDATED_AGENTS AS target
USING (
    SELECT
        UPPER(TRIM(RAW:agent_id::STRING))         AS agent_id,
        INITCAP(TRIM(RAW:agent_name::STRING))     AS agent_name,
        INITCAP(TRIM(RAW:region::STRING))         AS region,
        LOWER(TRIM(RAW:email::STRING))            AS email,
        TRIM(RAW:phone::STRING)                   AS phone,
        UPPER(TRIM(RAW:license_status::STRING))   AS license_status,
        TRY_TO_DATE(RAW:joining_date::STRING)     AS joining_date,
        UPPER(TRIM(RAW:branch_code::STRING))      AS branch_code
    FROM RAW_ABC_SCHEMA.RAW_AGENTS
    WHERE RAW:agent_id IS NOT NULL

    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY RAW:agent_id::STRING
            ORDER BY TRY_TO_DATE(RAW:joining_date::STRING) DESC
        ) = 1
) AS source

ON target.agent_id = source.agent_id

WHEN MATCHED THEN UPDATE SET
    target.agent_name      = source.agent_name,
    target.region          = source.region,
    target.email           = source.email,
    target.phone           = source.phone,
    target.license_status  = source.license_status,
    target.joining_date    = source.joining_date,
    target.branch_code     = source.branch_code,
    target.updated_at      = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    agent_id, agent_name, region, email,
    phone, license_status, joining_date, branch_code
)
VALUES (
    source.agent_id, source.agent_name, source.region,
    source.email, source.phone, source.license_status,
    source.joining_date, source.branch_code
);
-----------------------------------------------------------------------------------------------------------------------------------------------------------------


ALTER TASK TASK_LOAD_BRONZE_TO_SILVER_FOR_AGENTS RESUME;
ALTER TASK TASK_LOAD_BRONZE_TO_SILVER_FOR_RISK RESUME;
------------------------------------------------------------------------------------------------------------------------------------------

-------FOR AUDIT CHANGES USING STREAM AND TASKS

---AUDIT TABLE

CREATE OR REPLACE TABLE AUDIT_LOG (
    log_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    event_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    table_name    STRING,
    action_type   STRING,
    is_update     BOOLEAN,
    record_id     STRING,
    performed_by  STRING DEFAULT CURRENT_USER(),
    remarks       STRING
);

---STREAM
CREATE OR REPLACE STREAM CREATING_STREAM_ON_SILVER_FOR_TRACKING_AGENTS
ON TABLE VALIDATED_AGENTS
APPEND_ONLY = FALSE
COMMENT = 'Tracks all changes to Silver Policy';


CREATE OR REPLACE STREAM CREATING_STREAM_ON_SILVER_FOR_TRACKING_RISKS
ON TABLE VALIDATED_RISK
APPEND_ONLY = FALSE
COMMENT = 'Tracks all changes to Silver Claim';
------------------------------------------------------------------------------------------------------------------------------------------

---TASK
CREATE OR REPLACE TASK TASK_AUDIT_SILVER_AGENTS
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA('CREATING_STREAM_ON_SILVER_FOR_TRACKING_AGENTS')
AS
INSERT INTO AUDIT_LOG (
    table_name,
    action_type,
    is_update,
    record_id,
    remarks
)
SELECT
    'VALIDATED_AGENTS' AS table_name,
    METADATA$ACTION  AS action_type,
    METADATA$ISUPDATE AS is_update,
    policy_number  AS record_id,
    'Change detected in Silver'  AS remarks
FROM CREATING_STREAM_ON_SILVER_FOR_TRACKING_AGENTS;
------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_AUDIT_SILVER_RISKS
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA('CREATING_STREAM_ON_SILVER_FOR_TRACKING_RISKS')
AS
INSERT INTO AUDIT_LOG (
    table_name,
    action_type,
    is_update,
    record_id,
    remarks
)
SELECT
    'VALIDATED_RISK'  AS table_name,
    METADATA$ACTION   AS action_type,
    METADATA$ISUPDATE  AS is_update,
    policy_number  AS record_id,
    'Change detected in Silver' AS remarks
FROM CREATING_STREAM_ON_SILVER_FOR_TRACKING_RISKS;
------------------------------------------------------------------------------------------------------------------------------------------

ALTER TASK TASK_LOAD_BRONZE_TO_SILVER_FOR_AGENTS RESUME;
ALTER TASK CREATING_STREAM_ON_SILVER_FOR_TRACKING_RISKS RESUME;

    

SELECT * FROM validated_abc_schema.VALIDATED_AGENTS;

SELECT * FROM validated_abc_schema.validated_customer;

UPDATE validated_abc_schema.validated_customer
SET NAME = 'HLL'
WHERE CUSTOMER_ID = 'C001';

SELECT * FROM validated_abc_schema.audit_log;
