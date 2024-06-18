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
    LANGUAGE SQL
    AS $$
    BEGIN
        CASE (ref_name)
            WHEN 'EGRESS_EAI_WIKIPEDIA' THEN
                -- Add EXTERNAL ACCESS INTEGRATION for upload.wikimedia.org
                RETURN '{"type": "CONFIGURATION", "payload": { "host_ports": ["upload.wikimedia.org"], "allowed_secrets": "NONE" } }';
        END;
        RETURN '{"type": "ERROR", "payload": "Unknown Reference"}';
    END;
    $$;
    GRANT USAGE ON PROCEDURE config.configuration_callback(STRING) TO APPLICATION ROLE app_admin;

CREATE PROCEDURE config.version_initializer()
    RETURNS boolean
    LANGUAGE SQL
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: version_initializer: initializing');
        
        CALL config.upgrade_all_services() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    EXCEPTION WHEN OTHER THEN
        RAISE;
    END;
    $$;

-- Prefix to use for all global objects created (e.g., COMPUTE POOLS, WAREHOUSES, etc)
CREATE OR REPLACE FUNCTION config.app_prefix(root STRING)
    RETURNS string
    AS $$
    UPPER(current_database() || '__' || root)
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
        SELECT BOOLOR_AGG(value) INTO :b 
            FROM TABLE(FLATTEN(
                TRANSFORM(PARSE_JSON(SYSTEM$GET_SERVICE_STATUS('FRONTEND')), a VARIANT -> a:status = 'SUSPENDED')
            ));
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

CREATE OR REPLACE PROCEDURE config.create_all_compute_pools()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_all_compute_pools: creating all compute pools');
        CALL config.permissions_and_references(ARRAY_CONSTRUCT('CREATE COMPUTE POOL'),
                                            ARRAY_CONSTRUCT()) INTO :b;
        IF (NOT b) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: create_all_compute_pools: Insufficient permissions');
            RETURN false;
        END IF;

        CALL config.create_compute_pool_stpool() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.create_all_compute_pools() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_all_compute_pools()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_all_compute_pools: dropping compute pools');

        CALL config.drop_compute_pool_stpool() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.drop_all_compute_pools() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_compute_pool_stpool()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        name STRING DEFAULT config.app_prefix('stpool');
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_compute_pool_stpool: creating compute pool ' || name);
        CALL config.permissions_and_references(ARRAY_CONSTRUCT('CREATE COMPUTE POOL'),
                                            ARRAY_CONSTRUCT()) INTO :b;
        IF (NOT b) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: create_compute_pool_stpool: Insufficient permissions');
            RETURN false;
        END IF;
        CREATE COMPUTE POOL IF NOT EXISTS Identifier(:name)
            MIN_NODES = 1 MAX_NODES = 1
            INSTANCE_FAMILY = CPU_X64_XS
            AUTO_RESUME = TRUE;
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_compute_pool_stpool: compute pool ' || name || ' dropped');
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_compute_pool_stpool: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.create_compute_pool_stpool() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_compute_pool_stpool()
    RETURNS OBJECT
    LANGUAGE sql
    AS $$
    DECLARE
        name STRING DEFAULT config.app_prefix('stpool');
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_compute_pool_stpool: dropping compute pool ' || name);
        DROP COMPUTE POOL IF EXISTS Identifier(:name);
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_compute_pool_stpool: compute pool ' || name || ' dropped');
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_compute_pool_stpool: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.drop_compute_pool_stpool() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_all_warehouses()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_all_warehouses: creating all warehouses');
        CALL config.permissions_and_references(ARRAY_CONSTRUCT('CREATE WAREHOUSE'),
                                            ARRAY_CONSTRUCT()) INTO :b;
        IF (NOT b) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: create_all_warehouses: Insufficient permissions');
            RETURN false;
        END IF;

        CALL config.create_warehouse_stwh() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.create_all_warehouses() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_all_warehouses()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_all_warehouses: dropping all warehouses');

        CALL config.drop_warehouse_stwh() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.drop_all_warehouses() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_warehouse_stwh()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        name STRING DEFAULT config.app_prefix('stwh');
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_warehouse_stwh: creating warehouse ' || name);
        CALL config.permissions_and_references(ARRAY_CONSTRUCT('CREATE WAREHOUSE'),
                                            ARRAY_CONSTRUCT()) INTO :b;
        IF (NOT b) THEN 
            SYSTEM$LOG_INFO('NA_ST_SPCS: create_warehouse_stwh: Insufficient privileges');
            RETURN false;
        END IF;

        CREATE WAREHOUSE IF NOT EXISTS Identifier(:name) WITH WAREHOUSE_SIZE='XSMALL';
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_warehouse_stwh: warehouse ' || name || ' created');
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_warehouse_stwh: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.create_warehouse_stwh() TO APPLICATION ROLE app_admin;


CREATE OR REPLACE PROCEDURE config.drop_warehouse_stwh()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        name STRING DEFAULT config.app_prefix('stwh');
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_warehouse_stwh: dropping warehouse ' || name);
        DROP WAREHOUSE IF EXISTS Identifier(:name);
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_warehouse_stwh: warehouse ' || name || ' dropped');
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_warehouse_stwh: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.drop_warehouse_stwh() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.create_all_services()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_all_services: creating all services');

        CALL config.create_service_st_spcs() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.create_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_all_services()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_all_services: dropping all services');

        CALL config.drop_service_st_spcs() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.drop_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.suspend_all_services()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: suspend_all_services: suspending all services');

        CALL config.suspend_service_st_spcs() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.suspend_all_services() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.resume_all_services()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: resume_all_services: resuming all services');

        CALL config.create_service_st_spcs() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.resume_all_services() TO APPLICATION ROLE app_admin;


CREATE OR REPLACE PROCEDURE config.upgrade_all_services()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: upgrade_all_services: upgrading all services');

        CALL config.upgrade_service_st_spcs() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: upgrade_all_services: ERROR: ' || SQLERRM);
        RAISE;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.resume_all_services() TO APPLICATION ROLE app_admin;
CREATE OR REPLACE PROCEDURE config.create_service_st_spcs()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        whname STRING DEFAULT config.app_prefix('stwh');
        poolname STRING DEFAULT config.app_prefix('stpool');
        b BOOLEAN;
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: starting');
 
        -- Make sure COMPUTE POOL exists
        CALL config.create_compute_pool_stpool() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;

        -- Make sure WAREHOUSE exists
        CALL config.create_warehouse_stwh() INTO :b;
        IF (NOT b) THEN
            RETURN false;
        END IF;
 
        -- Check that BIND SERVICE ENDPOINT has been granted
        -- Check that EGRESS_EAI_WIKIPEDIA reference has been set
        -- Check that ORDERS_TABLE reference has been set
        --     FOR NOW, don't check the ORDERS_TABLE, it can't be set at setup, 
        --       but this is the default_web_endpoint and MUST be created based
        --       solely on the permissions and references that can be granted at setup.
        -- CALL config.permissions_and_references(ARRAY_CONSTRUCT('BIND SERVICE ENDPOINT'),
        --                                     ARRAY_CONSTRUCT('ORDERS_TABLE', 'EGRESS_EAI_WIKIPEDIA')) INTO :b;
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: checking if we have all permissions and references');
        CALL config.permissions_and_references(ARRAY_CONSTRUCT('BIND SERVICE ENDPOINT'),
                                            ARRAY_CONSTRUCT('EGRESS_EAI_WIKIPEDIA')) INTO :b;
        IF (NOT b) THEN
            SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: Insufficient permissions');
            RETURN false;
        END IF;

        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: starting service');

        -- FOR NOW, we need to do this as EXECUTE IMMEDIATE
        --    QUERY_WAREHOUSE doesn't take Identifier()
        -- CREATE SERVICE IF NOT EXISTS app_public.st_spcs
        --     IN COMPUTE POOL Identifier(:poolname)
        --     FROM SPECIFICATION_FILE='/streamlit.yaml'
        --     EXTERNAL_ACCESS_INTEGRATIONS=( Reference('EGRESS_EAI_WIKIPEDIA') )
        --     QUERY_WAREHOUSE=Identifier(:whname)
        -- ;
        LET q STRING := 'CREATE SERVICE IF NOT EXISTS app_public.st_spcs
            IN COMPUTE POOL Identifier(''' || poolname || ''')
            FROM SPECIFICATION_FILE=''/streamlit.yaml''
            EXTERNAL_ACCESS_INTEGRATIONS=( Reference(''EGRESS_EAI_WIKIPEDIA'') )
            QUERY_WAREHOUSE=''' || whname || '''';
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: Command: ' || q);
        EXECUTE IMMEDIATE q;


        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: waiting on service start');
        SELECT SYSTEM$WAIT_FOR_SERVICES(300, 'APP_PUBLIC.ST_SPCS');
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: granting usage');
        GRANT SERVICE ROLE app_public.st_spcs!app TO APPLICATION ROLE app_user;

        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: finished!');
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: create_service_st_spcs: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.create_service_st_spcs() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.drop_service_st_spcs()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_service_st_spcs: dropping service ST_SPCS');

        DROP SERVICE IF EXISTS app_public.st_spcs;
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: drop_service_st_spcs: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.drop_service_st_spcs() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.suspend_service_st_spcs()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: suspend_service_st_spcs: suspending service ST_SPCS');

        ALTER SERVICE IF EXISTS app_public.st_spcs SUSPEND;
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: suspend_service_st_spcs: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.suspend_service_st_spcs() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.resume_service_st_spcs()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: resume_service_st_spcs: resuming service ST_SPCS');

        ALTER SERVICE IF EXISTS app_public.st_spcs RESUME;
        RETURN true;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: resume_service_st_spcs: ERROR: ' || SQLERRM);
        RETURN false;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.resume_service_st_spcs() TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE config.upgrade_service_st_spcs()
    RETURNS boolean
    LANGUAGE sql
    AS $$
    DECLARE
        whname STRING DEFAULT config.app_prefix('stwh');
        poolname STRING DEFAULT config.app_prefix('stpool');
        b BOOLEAN;
        b2 BOOLEAN;
        suspended BOOLEAN;
        UPGRADE_ST_SERVICES_EXCEPTION EXCEPTION (-20002, 'Error upgrading ST_SPCS');
    BEGIN
        SYSTEM$LOG_INFO('NA_ST_SPCS: upgrade_service_st_spcs: upgrading service ST_SPCS');

        -- See if service exists
        CALL config.service_exists('APP_PUBLIC.ST_SPCS') INTO :b;
        IF (b) THEN
            -- See if service is suspended. If so, suspend service at the end
            CALL config.service_suspended('APP_PUBLIC.ST_SPCS') INTO :suspended;

            -- Alter the service
            -- ALTER SERVICE app_public.st_spcs FROM SPECIFICATION_FILE='/streamlit.yaml';
            EXECUTE IMMEDIATE 'ALTER SERVICE app_public.st_spcs FROM SPECIFICATION_FILE=''/streamlit.yaml''';

            -- ALTER SERVICE app_public.st_spcs SET
            --     EXTERNAL_ACCESS_INTEGRATIONS=( Reference('EGRESS_EAI_WIKIPEDIA') )
            --     QUERY_WAREHOUSE=Identifier(:whname)
            -- ;
            EXECUTE IMMEDIATE 'ALTER SERVICE app_public.st_spcs SET
                EXTERNAL_ACCESS_INTEGRATIONS=( Reference(''EGRESS_EAI_WIKIPEDIA'') )
                QUERY_WAREHOUSE=''' || whname || '''';

            -- Resume the service (to pick up any initialization logic that might be 
            --   in the new container image)
            CALL config.resume_service_st_spcs() INTO :b2;
            IF (NOT b2) THEN
                RAISE UPGRADE_ST_SERVICES_EXCEPTION;
            END IF;

            SYSTEM$LOG_INFO('NA_ST_SPCS: upgrade_service_st_spcs: waiting on service start');
            SELECT SYSTEM$WAIT_FOR_SERVICES(300, 'APP_PUBLIC.ST_SPCS');

            IF (suspended) THEN
                SYSTEM$LOG_INFO('NA_ST_SPCS: upgrade_service_st_spcs: re-suspending service');
                CALL config.suspend_service_st_spcs() INTO :b2;
                IF (NOT b2) THEN
                    RAISE UPGRADE_ST_SERVICES_EXCEPTION;
                END IF;
            END IF;
        END IF;
    EXCEPTION WHEN OTHER THEN
        SYSTEM$LOG_INFO('NA_ST_SPCS: upgrade_service_st_spcs: ERROR: ' || SQLERRM);
        RAISE;
    END
    $$;
    GRANT USAGE ON PROCEDURE config.resume_service_st_spcs() TO APPLICATION ROLE app_admin;




