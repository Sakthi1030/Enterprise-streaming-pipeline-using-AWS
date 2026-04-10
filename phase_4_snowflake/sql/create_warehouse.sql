-- ============================================
-- CREATE WAREHOUSE FOR SALES PLATFORM
-- ============================================

-- Create warehouse with appropriate size for workload
CREATE OR REPLACE WAREHOUSE SALES_WH
    WITH
    WAREHOUSE_SIZE = 'X-SMALL' -- Start small, scale as needed
    AUTO_SUSPEND = 300 -- Suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE -- Automatically resume when queries come in
    INITIALLY_SUSPENDED = TRUE -- Don't keep running when not in use
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3 -- Allow scaling for concurrent workloads
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for Real-Time Sales Platform analytics';

-- Create separate warehouse for ETL/processing
CREATE OR REPLACE WAREHOUSE ETL_WH
    WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 600 -- Suspend after 10 minutes (ETL runs less frequently)
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY' -- Save costs on ETL
    COMMENT = 'Warehouse for ETL/Data Loading operations';

-- Create warehouse for BI/Reporting
CREATE OR REPLACE WAREHOUSE BI_WH
    WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1 -- Single cluster for consistent performance
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for BI/Reporting queries';

-- Grant usage to appropriate roles
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE DEVELOPER;

GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE DEVELOPER;

GRANT USAGE ON WAREHOUSE BI_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE BI_WH TO ROLE ANALYST;

-- Show created warehouses
SHOW WAREHOUSES LIKE '%SALES%';

-- ============================================
-- CREATE ROLES FOR ACCESS CONTROL
-- ============================================

-- Create role hierarchy
CREATE OR REPLACE ROLE SALES_ADMIN;
CREATE OR REPLACE ROLE SALES_DEVELOPER;
CREATE OR REPLACE ROLE SALES_ANALYST;
CREATE OR REPLACE ROLE SALES_READ_ONLY;

-- Grant role hierarchy
GRANT ROLE SALES_DEVELOPER TO ROLE SALES_ADMIN;
GRANT ROLE SALES_ANALYST TO ROLE SALES_ADMIN;
GRANT ROLE SALES_READ_ONLY TO ROLE SALES_ANALYST;

-- Grant warehouse access to roles
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE SALES_ADMIN;
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE SALES_DEVELOPER;
GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE SALES_ANALYST;

GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE SALES_ADMIN;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE SALES_DEVELOPER;

GRANT USAGE ON WAREHOUSE BI_WH TO ROLE SALES_ADMIN;
GRANT USAGE ON WAREHOUSE BI_WH TO ROLE SALES_ANALYST;

-- ============================================
-- CREATE USERS AND ASSIGN ROLES
-- ============================================

-- Create service account for Airflow ETL
CREATE OR REPLACE USER AIRFLOW_SERVICE
    PASSWORD = 'ChangeThisPassword123!'  -- Change in production
    DEFAULT_ROLE = SALES_DEVELOPER
    DEFAULT_WAREHOUSE = ETL_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for Airflow ETL processes';

-- Create BI/Reporting user
CREATE OR REPLACE USER BI_USER
    PASSWORD = 'ChangeThisPassword123!'
    DEFAULT_ROLE = SALES_ANALYST
    DEFAULT_WAREHOUSE = BI_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'User for BI/Reporting tools';

-- Create analyst user
CREATE OR REPLACE USER ANALYST_USER
    PASSWORD = 'ChangeThisPassword123!'
    DEFAULT_ROLE = SALES_ANALYST
    DEFAULT_WAREHOUSE = BI_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'User for data analysts';

-- Assign roles to users
GRANT ROLE SALES_DEVELOPER TO USER AIRFLOW_SERVICE;
GRANT ROLE SALES_ANALYST TO USER BI_USER;
GRANT ROLE SALES_ANALYST TO USER ANALYST_USER;

-- ============================================
-- MONITORING AND OPTIMIZATION SETTINGS
-- ============================================

-- Enable query acceleration for large queries
ALTER WAREHOUSE SALES_WH SET
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;

-- Set statement timeout
ALTER WAREHOUSE SALES_WH SET
    STATEMENT_TIMEOUT_IN_SECONDS = 7200; -- 2 hours for long-running queries

ALTER WAREHOUSE ETL_WH SET
    STATEMENT_TIMEOUT_IN_SECONDS = 10800; -- 3 hours for ETL

ALTER WAREHOUSE BI_WH SET
    STATEMENT_TIMEOUT_IN_SECONDS = 3600; -- 1 hour for BI queries

-- Enable resource monitors
CREATE OR REPLACE RESOURCE MONITOR SALES_RM
    WITH
    CREDIT_QUOTA = 100 -- Monthly credit quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

CREATE OR REPLACE RESOURCE MONITOR ETL_RM
    WITH
    CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 95 PERCENT DO SUSPEND;

-- Assign resource monitors to warehouses
ALTER WAREHOUSE SALES_WH SET RESOURCE_MONITOR = SALES_RM;
ALTER WAREHOUSE ETL_WH SET RESOURCE_MONITOR = ETL_RM;

-- ============================================
-- CREATE STORAGE INTEGRATION FOR S3
-- ============================================

-- Create storage integration for S3 (if using external stages)
CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-access-role' -- Replace with your ARN
    STORAGE_ALLOWED_LOCATIONS = ('s3://sales-platform-raw-data-dev/', 's3://sales-platform-processed-data-dev/');

-- Get integration details for AWS configuration
DESC INTEGRATION AWS_S3_INTEGRATION;

-- Grant integration usage
GRANT USAGE ON INTEGRATION AWS_S3_INTEGRATION TO ROLE SALES_DEVELOPER;
GRANT USAGE ON INTEGRATION AWS_S3_INTEGRATION TO ROLE SALES_ADMIN;

-- ============================================
-- FINAL SETUP SUMMARY
-- ============================================

-- Show all created resources
SELECT 'WAREHOUSES' as resource_type, count(*) as count FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2))) WHERE "name" LIKE '%SALES%' OR "name" LIKE '%ETL%' OR "name" LIKE '%BI%'
UNION ALL
SELECT 'ROLES', count(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-3))) WHERE "name" LIKE '%SALES%'
UNION ALL
SELECT 'USERS', count(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-4))) WHERE "name" LIKE '%AIRFLOW%' OR "name" LIKE '%BI%' OR "name" LIKE '%ANALYST%';

-- Display setup instructions
SELECT
    'SETUP COMPLETE' as message,
    '1. Copy AWS External ID from DESC INTEGRATION command' as step1,
    '2. Configure AWS IAM role with that External ID' as step2,
    '3. Update STORAGE_AWS_ROLE_ARN with your actual ARN' as step3,
    '4. Run create_database.sql next' as step4;