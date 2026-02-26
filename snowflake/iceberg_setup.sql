-- =============================================================================
-- Snowflake Iceberg Table Setup for Auth Transaction Data
--
-- Creates a storage integration, external volume, Iceberg table, and loads
-- auth data from S3.
--
-- Prerequisites:
--   1. Terraform applied (infra/iceberg.tf) — creates S3 bucket and IAM role
--   2. CSV uploaded to s3://pme-hackathon-iceberg-651767347247/raw/auth_data/
--
-- After running CREATE STORAGE INTEGRATION, you must complete the IAM trust
-- policy bootstrap (Step 2) before the external volume will work.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 0: Create database and warehouse
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS PME_HACKATHON;
USE DATABASE PME_HACKATHON;

CREATE SCHEMA IF NOT EXISTS PME_HACKATHON.PUBLIC;
USE SCHEMA PME_HACKATHON.PUBLIC;

CREATE WAREHOUSE IF NOT EXISTS PME_HACKATHON_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE PME_HACKATHON_WH;

-- ---------------------------------------------------------------------------
-- Step 1: Create Storage Integration
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION pme_hackathon_iceberg_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::651767347247:role/pme-hackathon-snowflake-iceberg'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = (
    's3://pme-hackathon-iceberg-651767347247/'
  );

-- ---------------------------------------------------------------------------
-- Step 2: Bootstrap IAM trust policy (one-time manual step)
-- ---------------------------------------------------------------------------
-- Run this to get the Snowflake principal ARN and external ID:

DESCRIBE STORAGE INTEGRATION pme_hackathon_iceberg_s3;

-- From the output, note:
--   STORAGE_AWS_IAM_USER_ARN   (e.g. arn:aws:iam::123456789012:user/xxxx)
--   STORAGE_AWS_EXTERNAL_ID    (e.g. ABC123_SFCRole=2_abcdefg...)
--
-- Then update the IAM role trust policy:
--
--   aws iam update-assume-role-policy \
--     --role-name pme-hackathon-snowflake-iceberg \
--     --policy-document '{
--       "Version": "2012-10-17",
--       "Statement": [{
--         "Effect": "Allow",
--         "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
--         "Action": "sts:AssumeRole",
--         "Condition": {
--           "StringEquals": {
--             "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
--           }
--         }
--       }]
--     }'
--
-- Wait ~60 seconds for IAM propagation before proceeding.

-- ---------------------------------------------------------------------------
-- Step 3: Create External Volume
-- ---------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL VOLUME pme_hackathon_iceberg_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'pme-hackathon-iceberg-s3'
      STORAGE_BASE_URL = 's3://pme-hackathon-iceberg-651767347247/iceberg/'
      STORAGE_PROVIDER = 'S3'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::651767347247:role/pme-hackathon-snowflake-iceberg'
    )
  );

-- Verify the external volume is accessible:
DESCRIBE EXTERNAL VOLUME pme_hackathon_iceberg_vol;

-- ---------------------------------------------------------------------------
-- Step 4: Create Iceberg Table
-- ---------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE PME_HACKATHON.PUBLIC.AUTH_DATA (
  auth_id   VARCHAR,
  xid       VARCHAR,
  auth_ts   TIMESTAMP_NTZ,
  merchant  VARCHAR,
  amount    FLOAT
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'pme_hackathon_iceberg_vol'
  BASE_LOCATION = 'auth_data/';

-- ---------------------------------------------------------------------------
-- Step 5: Create staging area and load data
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STAGE pme_hackathon_auth_stage
  STORAGE_INTEGRATION = pme_hackathon_iceberg_s3
  URL = 's3://pme-hackathon-iceberg-651767347247/raw/auth_data/'
  FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  );

-- Verify the stage can see the file:
LIST @pme_hackathon_auth_stage;

-- Load data:
COPY INTO PME_HACKATHON.PUBLIC.AUTH_DATA (auth_id, xid, auth_ts, merchant, amount)
  FROM @pme_hackathon_auth_stage
  FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  )
  ON_ERROR = 'ABORT_STATEMENT';

-- Verify:
SELECT COUNT(*) FROM PME_HACKATHON.PUBLIC.AUTH_DATA;
-- Expected: 532 rows

SELECT * FROM PME_HACKATHON.PUBLIC.AUTH_DATA LIMIT 5;
