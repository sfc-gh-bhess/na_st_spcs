import logging
import yaml
import json
from functools import cmp_to_key
import os
import sys
import traceback

import logging
logger = logging.getLogger("python_logger")

def load_objects():
    try:
        with open(os.path.join(sys._xoptions["snowflake_import_directory"], 'objects.yml'), "r") as f:
            y = yaml.safe_load(f)
            return y
        # y = yaml.safe_load(open(os.path.join(sys._xoptions["snowflake_import_directory"], 'objects.yml'), 'r'))
        # return y
    except Exception as e:
        logger.info(f"NA_ST_SPCS: load_objects: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")

def app_prefix(session, root):
    app = session.get_current_database().lstrip('\"').rstrip('\"')
    return f"{app}__{root}"

def configuration_callback(session, ref_name):
    logger.info(f"NA_ST_SPCS: configuration_callback({ref_name}): Starting")
    try:
        # y = yaml.safe_load(open('objects.yml', 'r'))
        y = load_objects()
        retval = json.dumps({'type': 'CONFIGURATION', 'payload': {k.lower():v for k,v in y[ref_name]['PARAMETERS'].items() } })
        logger.info(f"NA_ST_SPCS: configuration_callback({ref_name}): Returning: {retval}")
        return retval
    except Exception as e:
        logger.info(f"NA_ST_SPCS: configuration_callback: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return '{"type": "ERROR", "payload": "Unknown Reference"}'

def create_all_application_roles(session):
    logger.info("NA_ST_SPCS: create_all_application_roles: creating all application roles")
    y = load_objects()
    try:
        for r in [k for k,v in y.items() if v['TYPE'] == 'APPLICATION_ROLE']:
            session.sql(f"CREATE APPLICATION ROLE IF NOT EXISTS {r}").collect()
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: create_all_application_roles: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def create_all_compute_pools(session):
    logger.info("NA_ST_SPCS: create_all_compute_pools: creating all compute pools")
    if not session.call("config.permissions_and_references", ['CREATE COMPUTE POOL'], []):
        logger.info("NA_ST_SPCS: create_compute_pool: Insufficient permissions")
        return False
    
    y = load_objects()
    for cp in [k for k,v in y.items() if v['TYPE'] == 'COMPUTE_POOL']:
        if not session.call("config.create_compute_pool", cp):
            return False
    return True

def drop_all_compute_pools(session):
    logger.info("NA_ST_SPCS: drop_all_compute_pools: creating all compute pools")
    y = load_objects()
    for cp in [k for k,v in y.items() if v['TYPE'] == 'COMPUTE_POOL']:
        if not session.call("config.drop_compute_pool", cp):
            return False
    return True

def create_compute_pool(session, name):
    logger.info(f"NA_ST_SPCS: create_compute_pool: creating compute pool {name}")
    if not session.call("config.permissions_and_references", ['CREATE COMPUTE POOL'], []):
        logger.info("NA_ST_SPCS: create_compute_pool: Insufficient permissions")
        return False
    
    try:
        y = load_objects()
        params = " ".join([f"{k}='{v}'" if type(v)==str else f"{k}={v}" for k,v in y[name]['PARAMETERS'].items()])
        pool_name = app_prefix(session, name)
        session.sql(f"CREATE COMPUTE POOL IF NOT EXISTS {pool_name} {params}").collect()
        logger.info(f"NA_ST_SPCS: create_compute_pool: compute pool {name} created")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: create_compute_pool: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def drop_compute_pool(session, name):
    logger.info(f"NA_ST_SPCS: drop_compute_pool: dropping compute pool {name}")
    try:
        pool_name = app_prefix(session, name)
        session.sql(f"DROP COMPUTE POOL IF EXISTS {pool_name}").collect()
        logger.info(f"NA_ST_SPCS: drop_compute_pool: compute pool {name} dropped")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: drop_compute_pool: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def create_all_warehouses(session):
    logger.info("NA_ST_SPCS: create_all_warehouses: creating all warehouses")
    if not session.call("config.permissions_and_references", ['CREATE WAREHOUSE'], []):
        logger.info("NA_ST_SPCS: create_all_warehouses: Insufficient permissions")
        return False
    
    y = load_objects()
    for cp in [k for k,v in y.items() if v['TYPE'] == 'WAREHOUSE']:
        if not session.call("config.create_warehouse", cp):
            return False
    return True;

def drop_all_warehouses(session):
    logger.info("NA_ST_SPCS: drop_all_warehouses: creating all warehouses")
    y = load_objects()
    for cp in [k for k,v in y.items() if v['TYPE'] == 'WAREHOUSE']:
        if not session.call("config.drop_warehouse", cp):
            return False
    return True;

def create_warehouse(session, name):
    logger.info(f"NA_ST_SPCS: create_warehouse: creating warehouse {name}")
    if not session.call("config.permissions_and_references", ['CREATE WAREHOUSE'], []):
        logger.info("NA_ST_SPCS: create_warehouse: Insufficient permissions")
        return False
    
    try:
        y = load_objects()
        params = " ".join([f"{k}='{v}'" if type(v)==str else f"{k}={v}" for k,v in y[name]['PARAMETERS'].items()])
        wh_name = app_prefix(session, name)
        session.sql(f"CREATE WAREHOUSE IF NOT EXISTS {wh_name} {params}").collect()
        logger.info(f"NA_ST_SPCS: create_warehouse: warehouse {name} created")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: create_warehouse: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def drop_warehouse(session, name):
    logger.info(f"NA_ST_SPCS: drop_warehouse: dropping warehouse {name}")
    try:
        wh_name = app_prefix(session, name)
        session.sql(f"DROP WAREHOUSE IF EXISTS {wh_name}").collect()
        logger.info(f"NA_ST_SPCS: drop_warehouse: warehouse {name} dropped")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: drop_warehouse: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def create_all_services(session):
    logger.info("NA_ST_SPCS: create_all_services: creating all services")
    if not session.call("config.permissions_and_references", ['CREATE WAREHOUSE'], []):
        logger.info("NA_ST_SPCS: create_all_services: Insufficient permissions")
        return False
    
    if not session.call("config.create_all_application_roles"):
        logger.info("NA_ST_SPCS: create_all_services: Error creating application roles")
        return False
    
    y = load_objects()
    svcs = [k for k,v in y.items() if v['TYPE'] == 'SERVICE']
    lam = lambda a,b: -2 if "DEPENDS" not in y[a] else 2 if "DEPENDS" not in y[b] else -1 if a in y[b]["DEPENDS"] else 1 if b in y[a]["DEPENDS"] else 0
    for svc in sorted(svcs, key=cmp_to_key(lam)):
        if not session.call("config.create_service", svc):
            return False
    return True;

def drop_all_services(session):
    logger.info("NA_ST_SPCS: drop_all_services: dropping all services")
    y = load_objects()
    svcs = [k for k,v in y.items() if v['TYPE'] == 'SERVICE']
    lam = lambda a,b: -2 if "DEPENDS" not in y[a] else 2 if "DEPENDS" not in y[b] else -1 if a in y[b]["DEPENDS"] else 1 if b in y[a]["DEPENDS"] else 0
    for svc in sorted(svcs, key=cmp_to_key(lam), reverse=True):
        if not session.call("config.drop_service", svc):
            return False
    return True;

def suspend_all_services(session):
    logger.info("NA_ST_SPCS: suspend_all_services: suspending all services")
    y = load_objects()
    svcs = [k for k,v in y.items() if v['TYPE'] == 'SERVICE']
    lam = lambda a,b: -2 if "DEPENDS" not in y[a] else 2 if "DEPENDS" not in y[b] else -1 if a in y[b]["DEPENDS"] else 1 if b in y[a]["DEPENDS"] else 0
    for svc in sorted(svcs, key=cmp_to_key(lam), reverse=True):
        if not session.call("config.suspend_service", svc):
            return False
    return True;

def resume_all_services(session):
    logger.info("NA_ST_SPCS: resume_all_services: dropping all services")
    y = load_objects()
    svcs = [k for k,v in y.items() if v['TYPE'] == 'SERVICE']
    lam = lambda a,b: -2 if "DEPENDS" not in y[a] else 2 if "DEPENDS" not in y[b] else -1 if a in y[b]["DEPENDS"] else 1 if b in y[a]["DEPENDS"] else 0
    for svc in sorted(svcs, key=cmp_to_key(lam)):
        if not session.call("config.resume_service", svc):
            return False
    return True;

def upgrade_all_services(session):
    logger.info("NA_ST_SPCS: upgrade_all_services: upgrading all services")
    y = load_objects()
    svcs = [k for k,v in y.items() if v['TYPE'] == 'SERVICE']
    lam = lambda a,b: -2 if "DEPENDS" not in y[a] else 2 if "DEPENDS" not in y[b] else -1 if a in y[b]["DEPENDS"] else 1 if b in y[a]["DEPENDS"] else 0
    for svc in sorted(svcs, key=cmp_to_key(lam)):
        if not session.call("config.upgrade_service", svc):
            return False
    return True;

def create_service(session, name):
    logger.info(f"NA_ST_SPCS: create_service: creating service {name}")
    try:
        y = load_objects()
        svc = y[name]
        svc_name = f"{svc['SCHEMA']}.{name}"
        ## Check COMPUTE POOL exists
        pool_name = svc["PARAMETERS"]["COMPUTE_POOL"]
        if not session.call("config.create_compute_pool", pool_name):
            logger.info("NA_ST_SPCS: create_service: Compute pool does not exist")
            return False
        pool_name = app_prefix(session, pool_name)
        ## If QUERY_WAREHOUSE, check if WAREHOUSE exists
        wh_name = None
        if "QUERY_WAREHOUSE" in svc["PARAMETERS"]:
            wh_name = svc["PARAMETERS"]["QUERY_WAREHOUSE"]
            if not session.call("config.create_warehouse", wh_name):
                logger.info("NA_ST_SPCS: create_service: Warehouse does not exist")
                return False
        wh_name = app_prefix(session, wh_name)
        ## Check that BIND SERVICE ENDPOINT has been granted
        ## Check that EGRESS_EAI_WIKIPEDIA reference has been set
        ## Check that ORDERS_TABLE reference has been set
        ##     FOR NOW, don't check the ORDERS_TABLE, it can't be set at setup, 
        ##       but this is the default_web_endpoint and MUST be created based
        ##       solely on the permissions and references that can be granted at setup.
        logger.info(f"NA_ST_SPCS: create_service: checking if we have all permissions and references for service {name}")
        perms = []
        refs = []
        if "PUBLIC_ENDPOINT" in svc["PARAMETERS"]:
            if svc["PARAMETERS"]["PUBLIC_ENDPOINT"]:
                perms.append('BIND SERVICE ENDPOINT')
        if "EXTERNAL_ACCESS_INTEGRATOINS" in svc["PARAMETERS"]:
            refs = [*refs, *svc["PARAMETERS"]["EXTERNAL_ACCESS_INTEGRATIONS"]]
        ## if "REFERENCES" in svc["PARAMETERS"]:
        ##     refs = [*refs, *svc["PARAMETERS"]["REFERENCES"]]
        if not session.call("config.permissions_and_references", ['CREATE COMPUTE POOL'], []):
            logger.info("NA_ST_SPCS: create_service: Insufficient permissions")
            return False

        qstr = f"""CREATE SERVICE IF NOT EXISTS {svc_name} 
                    IN COMPUTE POOL {pool_name}
                    FROM SPECIFICATION_FILE = '{svc["PARAMETERS"]["SPECIFICATION_FILE"]}'
                """
        if "EXTERNAL_ACCESS_INTEGRATOINS" in svc["PARAMETERS"]:
            eais = ",".join([f"Reference('{x}')" for x in svc["PARAMETERS"]["EXTERNAL_ACCESS_INTEGRATIONS"]])
            qstr = f"""{qstr}
                    EXTERNAL_ACCESS_INTEGRATIONS = ( {eais} )
                    """
        if wh_name:
            # qstr = f"""{qstr}
            #         QUERY_WAREHOUSE = Identifier('{wh_name}')
            #         """
            qstr = f"""{qstr}
                    QUERY_WAREHOUSE = '{wh_name}'
                    """
        params = " ".join([f"{k}='{v}'" if type(v)==str else f"{k}={v}" for k,v in y[name]['PARAMETERS'].items()
                            if k not in ["EXTERNAL_ACCESS_INTEGRATIONS", "PUBLIC_ENDPOINT", "REFERENCES", "COMPUTE_POOL", "SPECIFICATION_FILE", "QUERY_WAREHOUSE"]
                        ])
        qstr = f"{qstr} {params}"

        session.sql(qstr).collect()
        logger.info(f"NA_ST_SPCS: upgrade_service: service {name}: waiting on service to start")
        for r in svc['APPLICATION_ROLES'] or []:
            session.sql(f"GRANT USAGE ON SERVICE {svc_name} TO APPLICATION ROLE {r}").collect()
        session.sql(f"SELECT SYSTEM$WAIT_FOR_SERVICES(300, '{svc_name}')").collect()            
        logger.info(f"NA_ST_SPCS: create_service: service {name} created")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: create_service: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def drop_service(session, name):
    logger.info(f"NA_ST_SPCS: drop_service: dropping service {name}")
    try:
        y = load_objects()
        svc_name = f"{y[name]['SCHEMA']}.{name}"
        session.sql(f"DROP SERVICE IF EXISTS {svc_name}").collect()
        logger.info(f"NA_ST_SPCS: drop_service: service {name} dropped")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: drop_service: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def suspend_service(session, name):
    logger.info(f"NA_ST_SPCS: suspend_service: suspending service {name}")
    try:
        y = load_objects()
        svc_name = f"{y[name]['SCHEMA']}.{name}"
        session.sql(f"ALTER SERVICE IF EXISTS {svc_name} SUSPEND").collect()
        logger.info(f"NA_ST_SPCS: suspend_service: service {name} suspended")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: suspend_service: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def resume_service(session, name):
    logger.info(f"NA_ST_SPCS: resume_service: resuming service {name}")
    try:
        y = load_objects()
        svc_name = f"{y[name]['SCHEMA']}.{name}"
        session.sql(f"ALTER SERVICE IF EXISTS {svc_name} RESUME").collect()
        logger.info(f"NA_ST_SPCS: upgrade_service: service {name}: waiting on service to start")
        session.sql(f"SELECT SYSTEM$WAIT_FOR_SERVICES(300, '{svc_name}')").collect()            
        logger.info(f"NA_ST_SPCS: resume_service: service {name} resumed")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: resume_service: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False

def upgrade_service(session, name):
    logger.info(f"NA_ST_SPCS: upgrade_service: upgrading service {name}")
    try:
        y = load_objects()
        svc = y[name]
        svc_name = f"{svc['SCHEMA']}.{name}"
        # See if service exists
        if session.call("config.service_exists", svc_name):
            # See if service is suspended - if so, we'll re-suspend at the end
            suspended = session.call("config.service_suspended")

            # Alter service
            qstr = f"""ALTER SERVICE IF EXISTS {svc_name} 
                    FROM SPECIFICATION_FILE = '{svc["PARAMETERS"]["SPECIFICATION_FILE"]}'
                """
            session.sql(qstr).collect()
            qstr = ""
            if "EXTERNAL_ACCESS_INTEGRATOINS" in svc["PARAMETERS"]:
                eais = ",".join([f"Reference('{x}')" for x in svc["PARAMETERS"]["EXTERNAL_ACCESS_INTEGRATIONS"]])
                qstr = f"""{qstr}
                        EXTERNAL_ACCESS_INTEGRATIONS = ( {eais} )
                        """
            if "QUERY_WAREHOUSE" in svc["PARAMETERS"]:
                wh_name = app_prefix(session, svc["PARAMETERS"]["QUERY_WAREHOUSE"])
                # qstr = f"""{qstr}
                #         QUERY_WAREHOUSE = Identifier('{wh_name}')
                #         """
                qstr = f"""{qstr}
                        QUERY_WAREHOUSE = '{wh_name}'
                        """
            params = " ".join([f"{k}='{v}'" if type(v)==str else f"{k}={v}" for k,v in y[name]['PARAMETERS'].items()
                                if k not in ["EXTERNAL_ACCESS_INTEGRATIONS", "PUBLIC_ENDPOINT", "REFERENCES", "COMPUTE_POOL", "SPECIFICATION_FILE", "QUERY_WAREHOUSE"]
                            ])
            if qstr or params:
                session.sql(f"ALTER SERVICE {svc_name} SET {qstr} {params}").collect()

            # Resume service
            if not session.call("config.resume_service", svc_name):
                # raise exception
                pass
            logger.info(f"NA_ST_SPCS: upgrade_service: service {name}: waiting on service to start")
            session.sql(f"SELECT SYSTEM$WAIT_FOR_SERVICES(300, '{svc_name}')").collect()
            
            # If service was suspended, re-suspend service
            if suspended:
                logger.info(f"NA_ST_SPCS: upgrade_service: service {name}: Re-suspending service")
                if not session.call("config.suspend_service", svc_name):
                    # raise exception
                    pass

        logger.info(f"NA_ST_SPCS: upgrade_service: service {name} upgraded")
        return True
    except Exception as e:
        logger.info(f"NA_ST_SPCS: upgrade_service: ERROR: {str(e)}")
        logger.info(f"NA_ST_SPCS: {sys._getframe().f_code.co_name}: Traceback: {traceback.format_exc()}")
        return False
