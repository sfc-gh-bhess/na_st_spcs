CREATE OR ALTER VERSIONED SCHEMA config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE app_admin;

-- CALLBACKS
CREATE PROCEDURE config.reference_callback(ref_name STRING, operation STRING, ref_or_alias STRING)
 RETURNS STRING
 LANGUAGE SQL
 AS $$
    DECLARE
        retstr STRING;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: reference_callback: ref_name=' || ref_name || ' operation=' || operation);
        CASE (operation)
            WHEN 'ADD' THEN
                SELECT system$set_reference(:ref_name, :ref_or_alias);
                -- When references are added, see if we can start services that aren't started already
                CALL config.create_all_services() INTO :retstr;
            WHEN 'REMOVE' THEN
                SELECT system$remove_reference(:ref_name);
                retstr := 'Reference removed';
            WHEN 'CLEAR' THEN
                SELECT system$remove_reference(:ref_name);
                retstr := 'Reference cleared';
            ELSE
                retstr := 'Unknown operation: ' || operation;
        END;
        RETURN retstr;
    END;
   $$;
    GRANT USAGE ON PROCEDURE config.reference_callback(STRING,  STRING,  STRING) TO APPLICATION ROLE app_admin;

CREATE PROCEDURE config.grant_callback(privs ARRAY)
    RETURNS string
    LANGUAGE SQL
    AS $$
    DECLARE
        retstr STRING;
    BEGIN
        IF (ARRAY_CONTAINS('CREATE COMPUTE POOL'::VARIANT, :privs)) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: creating all compute pools');
            CALL config.create_all_compute_pools() INTO :retstr;
            SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: compute pools: ' || :retstr);
        END IF;
        IF (ARRAY_CONTAINS('CREATE WAREHOUSE'::VARIANT, :privs)) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: creating all warehouses');
            CALL config.create_all_warehouses() INTO :retstr;
            SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: warehouses: ' || :retstr);
        END IF;
        -- Whenever grants are added, see if we can start services that aren't started already
        SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: creating all services');
        CALL config.create_all_services() INTO :retstr;
        SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: services: ' || :retstr);
        RETURN retstr;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: grant_callback: EXCEPTION: ' || SQLERRM);
    END;
    $$;
    GRANT USAGE ON PROCEDURE config.grant_callback(ARRAY) TO APPLICATION ROLE app_admin;

CREATE PROCEDURE config.configuration_callback(ref_name STRING)
    RETURNS string
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.configuration_callback'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml')
    ;
    GRANT USAGE ON PROCEDURE config.configuration_callback(STRING) TO APPLICATION ROLE app_admin;

CREATE PROCEDURE config.version_initializer()
    RETURNS boolean
    LANGUAGE SQL
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: version_initializer: initializing');



        -- For now, just return
        RETURN true;

        CALL config.upgrade_all_services() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    EXCEPTION WHEN OTHER THEN
        RAISE;
    END;
    $$;

CREATE OR REPLACE PROCEDURE config.compute_pool_exists(name STRING)
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SELECT SYSTEM$GET_COMPUTE_POOL_STATUS(:name) IS NOT NULL INTO :b;
        SYSTEM$LOG_INFO('NA_ST_SPCS: compute_pool_exists: Service found? ' || b);
        RETURN b;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: compute_pool_exists: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;

CREATE OR REPLACE PROCEDURE config.warehouse_exists(name STRING)
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        ct INTEGER;
    BEGIN
        -- TODO: This doesn't actually work since we don't have access to LAST_QUERY_ID on the config page
        EXECUTE IMMEDIATE 'SHOW WAREHOUSES';
        SELECT COUNT(*) INTO :ct FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" = :name;
        IF (ct > 0) THEN
            RETURN true;
        END IF;
        RETURN false;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: warehouse_exists: ERROR: ' || SQLERRM);
    END
    $$;

CREATE OR REPLACE PROCEDURE config.service_exists(name STRING)
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        ct INTEGER;
    BEGIN
        SELECT ARRAY_SIZE(PARSE_JSON(SYSTEM$GET_SERVICE_STATUS(:name))) INTO ct;
        IF (ct > 0) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: service_exists: Service found');
            RETURN true;
        END IF;
        SYSTEM$LOG_INFO('NA_ST_SPCS: service_exists: Did not find service');
        RETURN false;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: service_exists: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;

CREATE OR REPLACE PROCEDURE config.service_suspended(name STRING)
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SELECT BOOLOR_AGG(value:status = 'SUSPENDED') INTO :b 
            FROM TABLE(FLATTEN(
                PARSE_JSON(SYSTEM$GET_SERVICE_STATUS(UPPER(:name)))
            ))
        ;
        SYSTEM$LOG_INFO('NA_ST_SPCS: service_suspended: Service suspended? ' || b);
        RETURN b;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: service_suspended: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;


-- Checks to see if the list of permissions and references have been set/granted.
CREATE OR REPLACE PROCEDURE config.permissions_and_references(perms ARRAY, refs ARRAY)
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        i INTEGER;
        len INTEGER;
    BEGIN
        FOR i IN 0 TO ARRAY_SIZE(perms)-1 DO
            LET p VARCHAR := GET(perms, i)::VARCHAR;
            IF (NOT SYSTEM$HOLD_PRIVILEGE_ON_ACCOUNT(:p)) THEN
                RETURN false;
            END IF;
        END FOR;

        FOR i IN 0 TO ARRAY_SIZE(refs)-1 DO
            LET p VARCHAR := GET(refs, i)::VARCHAR;
            SELECT ARRAY_SIZE(PARSE_JSON(SYSTEM$GET_ALL_REFERENCES(:p))) INTO :len;
            IF (len < 1) THEN
                RETURN false;
            END IF;
        END FOR;

        RETURN true;
    END
    $$;

CREATE OR REPLACE PROCEDURE config.create_all_application_roles()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_all_application_roles'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_all_application_roles() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_all_compute_pools()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_all_compute_pools'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_all_compute_pools() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_all_compute_pools()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.drop_all_compute_pools'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.drop_all_compute_pools() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_compute_pool(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_compute_pool'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_compute_pool(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_compute_pool(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.drop_compute_pool'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.drop_compute_pool(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_all_warehouses()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_all_warehouses'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_all_warehouses() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_all_warehouses()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.drop_all_warehouses'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.drop_all_warehouses() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_warehouse(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_warehouse'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_warehouse(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_warehouse(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.drop_warehouse'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.drop_warehouse(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_all_services()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_all_services'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_all_services()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.drop_all_services'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.drop_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.suspend_all_services()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.suspend_all_services'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.suspend_all_services() TO APPLICATION ROLE app_admin;


CREATE OR REPLACE PROCEDURE config.resume_all_services()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.resume_all_services'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.resume_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.upgrade_all_services()
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.upgrade_all_services'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.resume_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_service(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.create_service'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.create_service(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_service(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.drop_service'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.drop_service(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.suspend_service(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.suspend_service'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.suspend_service(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.resume_service(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.resume_service'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.resume_service(VARCHAR) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.upgrade_service(name VARCHAR)
    RETURNS boolean
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.10
    HANDLER = 'config.upgrade_service'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
    IMPORTS = ('/config.py', '/objects.yml');
    GRANT USAGE ON PROCEDURE config.upgrade_service(VARCHAR) TO APPLICATION ROLE app_admin;
