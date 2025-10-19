
-- snowflake/setup.sql
-- ------------------------------------------------------------------
-- Fintech Live Payments – end-to-end Snowflake setup (RAW→SILVER→GOLD)
-- Replace the <PLACEHOLDER> values before running.
-- ------------------------------------------------------------------

-- 0) Admin context
USE ROLE ACCOUNTADMIN;

-- 1) Compute & DB
CREATE OR REPLACE WAREHOUSE FINTECH_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE DATABASE FINTECH_DB;
CREATE OR REPLACE SCHEMA  FINTECH_DB.RAW;
CREATE OR REPLACE SCHEMA  FINTECH_DB.SILVER;
CREATE OR REPLACE SCHEMA  FINTECH_DB.GOLD;

USE WAREHOUSE FINTECH_WH;
USE DATABASE  FINTECH_DB;

-- 2) File format (CSV)
CREATE OR REPLACE FILE FORMAT FINTECH_DB.RAW.CSV_FMT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null');

-- 3) Storage integration (S3)
-- NOTE: Update the IAM role and bucket path.
CREATE OR REPLACE STORAGE INTEGRATION S3_FINTECH_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_IAM_ROLE_ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET_NAME>/data/transactions/');

-- optional: DESCRIBE INTEGRATION S3_FINTECH_INT;

-- 4) External stage
CREATE OR REPLACE STAGE FINTECH_DB.RAW.S3_TRX_STAGE
  STORAGE_INTEGRATION = S3_FINTECH_INT
  URL = 's3://<YOUR_BUCKET_NAME>/data/transactions/'
  FILE_FORMAT = FINTECH_DB.RAW.CSV_FMT;

-- optional: DESC STAGE FINTECH_DB.RAW.S3_TRX_STAGE;
-- Copy the NOTIFICATION_CHANNEL value and wire your S3 bucket events to that SQS.
-- (S3 -> Event to SQS the stage exposes; required for AUTO_INGEST to work.)

-- 5) RAW table (column names align with your SILVER/GOLD views and Power BI)
USE SCHEMA FINTECH_DB.RAW;

CREATE OR REPLACE TABLE TRANSACTIONS_RAW (
  TXN_ID          STRING,
  TXN_TS_UTC      TIMESTAMP_NTZ,         -- UTC event time
  CUSTOMER_ID     STRING,
  REGION          STRING,
  MERCHANT        STRING,
  PAYMENT_METHOD  STRING,                 -- CARD / WALLET / UPI / BANK_TRANSFER
  CURRENCY        STRING,
  AMOUNT_USD      NUMBER(12,2),
  STATUS          STRING,                 -- APPROVED / DECLINED / REFUNDED
  IS_REFUND       BOOLEAN,                -- true if refunded
  INGESTED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 6) Auto-ingest PIPE from S3
-- Edit COLUMN list to match your CSV order. Example assumes this order:
-- TXN_ID, Txn_Timestamp_UTC, CUSTOMER_ID, REGION, MERCHANT, PAYMENT_METHOD, CURRENCY,
-- AMOUNT_USD, STATUS, IS_REFUND
CREATE OR REPLACE PIPE FINTECH_DB.RAW.TRX_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO FINTECH_DB.RAW.TRANSACTIONS_RAW
  (
    TXN_ID,
    TXN_TS_UTC,
    CUSTOMER_ID,
    REGION,
    MERCHANT,
    PAYMENT_METHOD,
    CURRENCY,
    AMOUNT_USD,
    STATUS,
    IS_REFUND
  )
  FROM (
    SELECT
      $1::STRING,
      TRY_TO_TIMESTAMP_NTZ($2),
      $3::STRING,
      $4::STRING,
      $5::STRING,
      $6::STRING,
      $7::STRING,
      TRY_TO_NUMBER($8, 12, 2),
      $9::STRING,
      IFF(LOWER($10) IN ('1','true','t','y','yes','refunded'), TRUE, FALSE)
    FROM @FINTECH_DB.RAW.S3_TRX_STAGE
  )
  FILE_FORMAT = (FORMAT_NAME = FINTECH_DB.RAW.CSV_FMT)
  ON_ERROR = 'CONTINUE';

-- 7) SILVER – clean, deduped, enriched view
USE SCHEMA FINTECH_DB.SILVER;

CREATE OR REPLACE VIEW V_TRANSACTIONS_CLEAN AS
SELECT
  TXN_ID,
  TXN_TS_UTC,
  TO_DATE(TXN_TS_UTC)                           AS TXN_DATE,
  DATE_TRUNC('minute', TXN_TS_UTC)              AS TXN_MINUTE,
  REGION,
  MERCHANT,
  CUSTOMER_ID,
  PAYMENT_METHOD,
  CURRENCY,
  AMOUNT_USD::NUMBER(12,2)                      AS AMOUNT_USD,
  IFF(IS_REFUND, -ABS(AMOUNT_USD), AMOUNT_USD)  AS AMOUNT_NET_USD,
  STATUS,
  IFF(STATUS='APPROVED', 1, 0)                  AS IS_APPROVED_INT,
  IFF(STATUS='DECLINED', 1, 0)                  AS IS_DECLINED_INT,
  INGESTED_AT
FROM (
  SELECT
    t.*,
    ROW_NUMBER() OVER (PARTITION BY TXN_ID ORDER BY INGESTED_AT DESC) AS rn
  FROM FINTECH_DB.RAW.TRANSACTIONS_RAW t
)
WHERE rn = 1
  AND TXN_ID IS NOT NULL;

-- 8) GOLD – KPI views used by the report
USE SCHEMA FINTECH_DB.GOLD;

-- A) KPIs for last 60 minutes (cards)
CREATE OR REPLACE VIEW V_KPIS_60M AS
SELECT
  COUNT(*)                                                       AS txn_count_60m,
  SUM(AMOUNT_USD)                                               AS amount_total_usd_60m,
  SUM(AMOUNT_NET_USD)                                           AS amount_net_usd_60m,
  AVG(AMOUNT_USD)                                               AS avg_ticket_usd_60m,
  SUM(IS_APPROVED_INT)                                          AS approved_txns_60m,
  SUM(IS_DECLINED_INT)                                          AS declined_txns_60m,
  SUM(IFF(STATUS='REFUNDED',1,0))                               AS refunds_cnt_60m,
  IFF(COUNT(*)=0, 0, SUM(IFF(STATUS='REFUNDED',1,0))/COUNT(*))  AS refund_rate_60m,
  IFF(COUNT(*)=0, 0, SUM(IS_DECLINED_INT)/COUNT(*))             AS decline_rate_60m,
  MIN(INGESTED_AT)                                              AS window_start_utc,
  MAX(INGESTED_AT)                                              AS window_end_utc
FROM FINTECH_DB.SILVER.V_TRANSACTIONS_CLEAN
WHERE INGESTED_AT >= DATEADD('minute', -60, CURRENT_TIMESTAMP());

-- B) Revenue per minute – last 24h (line)
CREATE OR REPLACE VIEW V_REVENUE_PER_MINUTE_24H AS
SELECT
  TXN_MINUTE,
  SUM(AMOUNT_NET_USD) AS AMOUNT_NET_USD,
  COUNT(*)            AS TXN_COUNT
FROM FINTECH_DB.SILVER.V_TRANSACTIONS_CLEAN
WHERE TXN_TS_UTC >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY TXN_MINUTE
ORDER BY TXN_MINUTE;

-- C) Merchant per minute – last 24h (bar / Top N)
CREATE OR REPLACE VIEW V_MERCHANT_MINUTE_24H AS
SELECT
  MERCHANT,
  TXN_MINUTE,
  SUM(AMOUNT_NET_USD) AS AMOUNT_NET_USD,
  COUNT(*)            AS TXN_COUNT
FROM FINTECH_DB.SILVER.V_TRANSACTIONS_CLEAN
WHERE TXN_TS_UTC >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY MERCHANT, TXN_MINUTE;

-- (Optional) Region per minute – last 24h
CREATE OR REPLACE VIEW V_REGION_MINUTE_24H AS
SELECT
  REGION,
  TXN_MINUTE,
  SUM(AMOUNT_NET_USD) AS AMOUNT_NET_USD,
  COUNT(*)            AS TXN_COUNT
FROM FINTECH_DB.SILVER.V_TRANSACTIONS_CLEAN
WHERE TXN_TS_UTC >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY REGION, TXN_MINUTE;

-- 9) Quick health checks you can run anytime
-- Pipe state + last ingest
SELECT PARSE_JSON(SYSTEM$PIPE_STATUS('FINTECH_DB.RAW.TRX_PIPE')):"executionState"::string AS state,
       PARSE_JSON(SYSTEM$PIPE_STATUS('FINTECH_DB.RAW.TRX_PIPE')):"lastIngestedTimestamp"::timestamp AS last_ingested_utc;

-- Recent file loads (last 60m)
SELECT FILE_NAME, ROW_COUNT, LAST_LOAD_TIME
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'FINTECH_DB.RAW.TRANSACTIONS_RAW',
    START_TIME=>DATEADD('minute',-60,CURRENT_TIMESTAMP())
  )
)
ORDER BY LAST_LOAD_TIME DESC;
