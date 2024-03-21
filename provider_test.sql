-- FOLLOW THE consumer_setup.sql TO SET UP THE TEST ON THE PROVIDER
USE ROLE nac;
USE WAREHOUSE wh_nac;

DROP APPLICATION IF EXISTS spcs_app_instance;
CREATE APPLICATION spcs_app_instance FROM APPLICATION PACKAGE spcs_app_pkg USING VERSION v1;
DROP COMPUTE POOL IF EXISTS pool_nac;
CREATE COMPUTE POOL pool_nac FOR APPLICATION spcs_app_instance
    MIN_NODES = 1 MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE;
GRANT USAGE ON COMPUTE POOL pool_nac TO APPLICATION spcs_app_instance;
DESCRIBE COMPUTE POOL pool_nac;

-- Grant permission(s) via Snowsight configuration UI
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION spcs_app_instance;
-- Grant access to the query warehouse via Snowsight configuration UI
GRANT USAGE ON WAREHOUSE wh_nac TO APPLICATION spcs_app_instance;
-- Grant access to the TPC-H data in SNOWFLAKE_SAMPLE_DATA
CALL spcs_app_instance.v1.register_single_callback(
  'ORDERS_TABLE' , 'ADD', SYSTEM$REFERENCE('TABLE', 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS', 'PERSISTENT', 'SELECT'));

-- Start the app
-- CALL spcs_app_instance.app_public.start_app('POOL_NAC');
CALL spcs_app_instance.app_public.start_app('POOL_NAC', 'WH_NAC');
GRANT APPLICATION ROLE spcs_app_instance.app_user TO ROLE sandbox;
CALL spcs_app_instance.app_public.app_url();


