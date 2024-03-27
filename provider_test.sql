-- FOLLOW THE consumer_setup.sql TO SET UP THE TEST ON THE PROVIDER
USE ROLE nac;
USE WAREHOUSE wh_nac;

-- Create the APPLICATION
DROP APPLICATION IF EXISTS na_st_spcs_app;
CREATE APPLICATION na_st_spcs_app FROM APPLICATION PACKAGE na_st_spcs_pkg USING VERSION v1;

-- Grant permission(s) via Snowsight configuration UI or via SQL
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION na_st_spcs_app;
-- Grant access to the TPC-H ORDERS view in NAC_TEST.DATA via Snowsight Configuraiton UI or via SQL
CALL na_st_spcs_app.v1.register_single_callback(
  'ORDERS_TABLE' , 'ADD', SYSTEM$REFERENCE('VIEW', 'NAC_TEST.DATA.ORDERS', 'PERSISTENT', 'SELECT'));
-- Grant access to the query warehouse via SQL
GRANT USAGE ON WAREHOUSE wh_nac TO APPLICATION na_st_spcs_app;

-- Create the COMPUTE POOL for the APPLICATION
DROP COMPUTE POOL IF EXISTS pool_nac;
CREATE COMPUTE POOL pool_nac FOR APPLICATION na_st_spcs_app
    MIN_NODES = 1 MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE;
GRANT USAGE ON COMPUTE POOL pool_nac TO APPLICATION na_st_spcs_app;
DESCRIBE COMPUTE POOL pool_nac;
-- Wait until COMPUTE POOL state returns `IDLE` or `ACTIVE`

-- Start the app
CALL na_st_spcs_app.app_public.start_app('POOL_NAC', 'WH_NAC');
-- Grant usage of the app to others
GRANT APPLICATION ROLE na_st_spcs_app.app_user TO ROLE sandbox;
-- Get the URL for the app
CALL na_st_spcs_app.app_public.app_url();
