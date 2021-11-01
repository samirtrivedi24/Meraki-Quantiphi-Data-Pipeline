import psycopg2
from dateutil import parser, tz
from airflow.hooks.base_hook import BaseHook
import json

# POSTGRES_CONN_ID = 'postgres_airflowDags'
# POSTGRESQL_DATABASE = 'airflow'
# POSTGRESQL_TABLE = 'MERAKI_DW_AIRFLOW_METADATA.SRC_CONFIG_MASTER'
# COL_NAME = [DAG_NAME,
#             Config_Master_key,
#             SRC_SYSTEM,
#             SRC_DATABASE,
#             SRC_SCHEMA,
#             SRC_TABLE,
#             SRC_FIELDS,
#             SRC_Field_VERSION_NO,
#             ACTIVE_IND,
#             ]


################################
#### ---  AWS Secrets Manager -  get the values
# 
# ############################## 

###global static conn
# conn= 

# def get_conn();
#     if conn != none ;
#         return conn
#     else
#         return create_postgresql_connection():


def create_postgresql_connection():
    """
        Postgresql Connection
    """
    postgres_conf = BaseHook.get_connection(POSTGRES_CONN_ID)
    psql_conn = psycopg2.connect(
        host=postgres_conf.host,
        database=POSTGRESQL_DATABASE,
        user=postgres_conf.login,
        password=postgres_conf.password
    )
    return psql_conn
#########################################################################################

def select_postgresql_SrcConfig(psql_conn, DAG_NAME):
    
    
    #### - for test
    #psql_conn = create_postgresql_connection()
    ###-  get_conn
    output = ''
    cursor = psql_conn.cursor()

    # select  DAG_NAME, Config_Master_key, SRC_SYSTEM, SRC_DATABASE, SRC_SCHEMA,
    #         SRC_TABLE, SRC_FIELDS, SRC_Field_VERSION_NO, ACTIVE_IND
    #         from
    #         MERAKI_DW_AIRFLOW_METADATA.SRC_CONFIG_MASTER SCM_1
    #         where
    #             DAG_NAME = %s  ##script Name
    #         and Active_ind = 'Y'

    POSTGRESQL_TABLE='MERAKI_DW_AIRFLOW_METADATA.SRC_CONFIG_MASTER'
    cols = [DAG_NAME, Config_Master_key, SRC_SYSTEM, SRC_DATABASE, SRC_SCHEMA,
             SRC_TABLE, SRC_FIELDS, SRC_Field_VERSION_NO, ACTIVE_IND]

    sql = """ SELECT {0} 
              FROM {1}
              WHERE  DAG_NAME ={2}
              and Active_ind = 'Y'
              """.format(COLS, POSTGRESQL_TABLE, DAG_NAME)
    try:
        cursor.execute(sql)
        records = cursor.fetchall()
        for row in records:
            output = row[0]
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
    #     if psql_conn is not None:
    #         psql_conn.close()
    return output
##############################################################################

 def insert_postgres_DagMstr(psql_conn, insert_parm):
    """
        Postgresql Inser query - One time Insert - Ist task at DAG execution to enter the DAG name 
    """


    #psql_conn = create_postgresql_connection()
    POSTGRESQL_TABLE = 'MERAKI_DW_AIRFLOW_METADATA.DAG_MASTER'
    # DAG_MASTER_NO
    # DAG_NAME	 -  pass it from DAG - (Global Parm)
    # DAG_ID	- pass it from DAG - (Global Parm)
    # RUN_DT    - 
    # START_TIME
    # END_TIME - No Inset (Update only)

    cursor = psql_conn.cursor()
    sql = """ INSERT INTO {0} 
            ( dag_name, dag_id, run_dt, start_time, end_time
            )
            VALUES (%s,%s,%s,%s,%s) returning DAG_MASTER_NO
          """.format(POSTGRESQL_TABLE)
    try:
        cursor.execute(sql,
                            (insert_param["dag_name"] ,
                             insert_param["dag_id"],
                             insert_param["run_dt"] ,
                             insert_param["start_time"]
                             ))
            records = cursor.fetchall()
        for row in records:
            DAG_MASTER_NO = row[0]
        cursor.close()
        # commit the changes
        psql_conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        DAG_MASTER_NO= None
    finally:
        if psql_conn is not None:
            psql_conn.close()

return DAG_MASTER_NO



####################################################################################


def update_postgres_DagMstr(psql_conn, DAG_MASTER_NO):
    """
        Postgresql Update query
    """
    #psql_conn = create_postgresql_connection()
    cursor = psql_conn.cursor()
    curr_time=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    POSTGRESQL_TABLE = MERAKI_DW_AIRFLOW_METADATA.DAG_RUN_MASTER
    
    params= (curr_time, DAG_MASTER_NO)
    sql = """update {0} set
    END_TIME= %s 
	where DAG_MASTER_NO = %s
                    """.format(POSTGRESQL_TABLE)
    try:
        cursor.execute(sql, params)
        cursor.close()
        # commit the changes
        psql_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

#####################################################################################################


def insert_postgres_TaskTracker(psql_conn, SRC_CONFIG_MASTER_KEY, insert_parm):
    """
        Postgresql Insert query for adhoc qa processing
    """
    #### call select function to execute 
    ##    select max(run_no) from MERAKI_DW_AIRFLOW_METADATA.DAG_RUN_TASK_TRACKER where 

    #psql_conn = create_postgresql_connection()
    psql_conn = create_postgresql_connection()
    POSTGRESQL_TABLE = 'MERAKI_DW_AIRFLOW_METADATA.DAG_RUN_TASK_TRACKER'
    # ,DAG_ID - Airflow dag-id  - must be part of insert_parm
    # ,TASK_ID- Airflow Task_id - must be part of insert_parm
    # ,RUN_DT        - curr date - must be part of insert_parm
    # ,RUN_NO        - NOT a part of insert_parm - Max(run no) + 1 for current date - call select func to get the max of run no
    # ,START_TIME        TIMESTAMP NOT NULL
    # ,END_TIME        TIMESTAMP NOT NULL
    # ,SRC_CONFIG_MASTER_KEY        - passed from Src_Config_Master Select Function 
    # ,REC_COUNT_ON_READ         -- Insert 0
    # ,REC_COUNT_ON_WRITE --- insert 0 
    cursor = psql_conn.cursor()
    sql = """ INSERT INTO {0} 
            ( DAG_ID, TASK_ID, RUN_DT, RUN_NO, START_TIME, END_TIME, SRC_CONFIG_MASTER_KEY,
              REC_COUNT_ON_READ, REC_COUNT_ON_WRITE
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) returning TASK_TRACKER_NO
          """.format(POSTGRESQL_TABLE)
    try:
        cursor.execute(sql,
                            (insert_param["DAG_ID"] ,
                             insert_param["TASK_ID"],
                             insert_param["RUN_DT"] ,
                             insert_param["RUN_NO"],
                             insert_param["START_TIME"] ,
                             insert_param["END_TIME"],
                             insert_param["SRC_CONFIG_MASTER_KEY"] ,
                             insert_param["REC_COUNT_ON_READ"],
                             insert_param["REC_COUNT_ON_WRITE"]
                             ))
            records = cursor.fetchall()
        for row in records:
            TASK_TRACKER_NO = row[0]
        cursor.close()
        # commit the changes
        psql_conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        TASK_TRACKER_NO= None
    finally:
        if psql_conn is not None:
            psql_conn.close()
    if TASK_TRACKER_NO:
        return TASK_TRACKER_NO


def update_postgres_TaskTracker(psql_conn, update_param):
    #### call select function to execute 
    ##    select max(run_no) from MERAKI_DW_AIRFLOW_METADATA.DAG_RUN_TASK_TRACKER where 

    psql_conn = create_postgresql_connection()
    POSTGRESQL_TABLE = 'MERAKI_DW_AIRFLOW_METADATA.DAG_RUN_TASK_TRACKER'
    cursor = psql_conn.cursor()

    sql = """ update {0} set
    REC_COUNT_ON_READ= %s,
    REC_COUNT_ON_WRITE = %s,
    END_TIME = %s, 
	where TASK_TRACKER_NO = %s
                    """.format(POSTGRESQL_TABLE)
         
    try:
        cursor.execute(sql, update_param["REC_COUNT_ON_READ"] ,
                            update_param["REC_COUNT_ON_WRITE"],
                            update_param["END_TIME"],
                            update_param["TASK_TRACKER_NO"])
            records = cursor.fetchall()
        for row in records:
            TASK_TRACKER_NO = row[0]
        cursor.close()
        # commit the changes
        psql_conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        TASK_TRACKER_NO= None
    finally:
        if psql_conn is not None:
            psql_conn.close()
    if TASK_TRACKER_NO:
        return TASK_TRACKER_NO