-- =============================================================================
-- Snowflake External Function Setup for PME Decryption
--
-- This script creates the API integration, external function, and example
-- queries for accessing PME-encrypted data via the AWS Lambda decryption proxy.
--
-- Prerequisites:
--   1. Terraform applied (infra/snowflake.tf) — creates API Gateway, Lambda,
--      and IAM roles
--   2. Lambda image deployed (./snowflake/deploy.sh)
--
-- After running CREATE API INTEGRATION, you must complete the IAM trust
-- policy bootstrap (Step 2 below) before the external function will work.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1: Create API Integration
-- ---------------------------------------------------------------------------
-- Replace <API_GATEWAY_INVOKE_URL> with the terraform output:
--   sf_decrypt_api_gateway_invoke_url (without the /decrypt suffix)
-- Replace <SNOWFLAKE_ROLE_ARN> with the terraform output:
--   sf_snowflake_role_arn

CREATE OR REPLACE API INTEGRATION pwe_hackathon_decrypt_api
  API_PROVIDER = aws_api_gateway
  API_AWS_ROLE_ARN = '<SNOWFLAKE_ROLE_ARN>'
  API_ALLOWED_PREFIXES = ('<API_GATEWAY_INVOKE_URL>')
  ENABLED = TRUE;

-- ---------------------------------------------------------------------------
-- Step 2: Bootstrap IAM trust policy (one-time manual step)
-- ---------------------------------------------------------------------------
-- Run this to get the Snowflake principal ARN and external ID:

DESCRIBE API INTEGRATION pwe_hackathon_decrypt_api;

-- From the output, note:
--   API_AWS_IAM_USER_ARN   (e.g. arn:aws:iam::123456789012:user/xxxx)
--   API_AWS_EXTERNAL_ID    (e.g. ABC123_SFCRole=2_abcdefg...)
--
-- Then update the IAM role trust policy for pme-hackathon-snowflake-apigw:
--
--   aws iam update-assume-role-policy \
--     --role-name pme-hackathon-snowflake-apigw \
--     --policy-document '{
--       "Version": "2012-10-17",
--       "Statement": [{
--         "Effect": "Allow",
--         "Principal": {"AWS": "<API_AWS_IAM_USER_ARN>"},
--         "Action": "sts:AssumeRole",
--         "Condition": {
--           "StringEquals": {
--             "sts:ExternalId": "<API_AWS_EXTERNAL_ID>"
--           }
--         }
--       }]
--     }'

-- ---------------------------------------------------------------------------
-- Step 3: Create External Function
-- ---------------------------------------------------------------------------
-- Replace <API_GATEWAY_INVOKE_URL> with the full URL including /decrypt suffix
-- from terraform output: sf_decrypt_api_gateway_invoke_url

CREATE OR REPLACE EXTERNAL FUNCTION pwe_hackathon_decrypt(
  row_index INT,
  role_name VARCHAR
)
  RETURNS VARIANT
  API_INTEGRATION = pwe_hackathon_decrypt_api
  MAX_BATCH_ROWS = 100
  AS '<API_GATEWAY_INVOKE_URL>';

-- ---------------------------------------------------------------------------
-- Step 4: Example queries — 3-tier RBAC
-- ---------------------------------------------------------------------------

-- Fraud Analyst (full access: all columns visible)
SELECT
  result:first_name::VARCHAR  AS first_name,
  result:last_name::VARCHAR   AS last_name,
  result:ssn::VARCHAR         AS ssn,
  result:email::VARCHAR       AS email,
  result:xid::VARCHAR         AS xid,
  result:balance::FLOAT       AS balance
FROM (
  SELECT pwe_hackathon_decrypt(SEQ4(), 'fraud-analyst') AS result
  FROM TABLE(GENERATOR(ROWCOUNT => 100))
);

-- Marketing Analyst (PCI denied: ssn is null)
SELECT
  result:first_name::VARCHAR  AS first_name,
  result:last_name::VARCHAR   AS last_name,
  result:ssn::VARCHAR         AS ssn,       -- will be null
  result:email::VARCHAR       AS email,
  result:xid::VARCHAR         AS xid,
  result:balance::FLOAT       AS balance
FROM (
  SELECT pwe_hackathon_decrypt(SEQ4(), 'marketing-analyst') AS result
  FROM TABLE(GENERATOR(ROWCOUNT => 100))
);

-- Junior Analyst (PCI + PII denied: ssn, first_name, last_name, email are null)
SELECT
  result:first_name::VARCHAR  AS first_name, -- will be null
  result:last_name::VARCHAR   AS last_name,  -- will be null
  result:ssn::VARCHAR         AS ssn,        -- will be null
  result:email::VARCHAR       AS email,      -- will be null
  result:xid::VARCHAR         AS xid,
  result:balance::FLOAT       AS balance
FROM (
  SELECT pwe_hackathon_decrypt(SEQ4(), 'junior-analyst') AS result
  FROM TABLE(GENERATOR(ROWCOUNT => 100))
);
