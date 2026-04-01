use database ABCD_DB;

CREATE OR REPLACE TABLE QUARANTINE_CLAIMS_RI_FAIL (
    CLAIM_ID STRING,
    POLICY_ID STRING,
    CLAIM_DATE DATE,
    REPORTED_DATE DATE,
    CLAIM_AMOUNT NUMBER,
    APPROVED_AMOUNT NUMBER,
    CLAIM_REASON STRING,
    CLAIM_STATUS STRING,
    SURVEYOR_ID STRING,
    IS_FLAGGED BOOLEAN,

    fail_reason STRING,
    quarantined_at TIMESTAMP,
    resolved_flag BOOLEAN DEFAULT FALSE
);

SELECT * FROM QUARANTINE_CLAIMS_RI_FAIL;

CREATE OR REPLACE PROCEDURE SP_RI_CHECK_CLAIMS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE v_count NUMBER;

BEGIN

    -- STEP 1: COUNT ORPHAN CLAIMS
    SELECT COUNT(*) INTO :v_count
    FROM validated_abc_schame.VALIDATED_CLAIMS c
    LEFT JOIN validated_abc_schema.VALIDATED_POLICY p
        ON c.POLICY_ID = p.POLICY_ID
    WHERE p.POLICY_ID IS NULL;

    -- STEP 2: IF FOUND → MOVE TO QUARANTINE
    IF (v_count > 0) THEN

        -- MOVE FAILED RECORDS
        INSERT INTO QUARANTINE_CLAIMS_RI_FAIL
        SELECT 
            c.CLAIM_ID,
            c.POLICY_ID,
            c.CLAIM_DATE,
            c.REPORTED_DATE,
            c.CLAIM_AMOUNT,
            c.APPROVED_AMOUNT,
            c.CLAIM_REASON,
            c.CLAIM_STATUS,
            c.SURVEYOR_ID,
            c.IS_FLAGGED,

            'No matching policy' AS fail_reason,
            CURRENT_TIMESTAMP(),
            FALSE
        FROM validated_abc_schema.VALIDATED_CLAIMS c
        LEFT JOIN validated_abc_schema.VALIDATED_POLICY p
            ON c.POLICY_ID = p.POLICY_ID
        WHERE p.POLICY_ID IS NULL;

        -- DELETE FROM MAIN TABLE
        DELETE FROM validated_abc_schema.VALIDATED_CLAIMS
        WHERE CLAIM_ID IN (
            SELECT CLAIM_ID
            FROM QUARANTINE_CLAIMS_RI_FAIL
            WHERE resolved_flag = FALSE
        );

        RETURN 'RI FAILED CLAIMS MOVED TO QUARANTINE';

    ELSE
        RETURN 'NO RI ISSUES FOUND';
    END IF;

END;
$$;

CALL SP_RI_CHECK_CLAIMS();



