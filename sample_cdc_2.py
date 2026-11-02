# Author: P.tourang
# Create: 9/1/2026
# Update: 9/2/2026


import datetime
import pytz
import jdatetime
import pymongo
import cx_Oracle
import sys
import time


sys.path.append(r"/home/admin/")
from secure_password import get_password
passw = get_password()


############################ Oracle Connection
oracleDB = cx_Oracle.connect()
oracle_cursor=oracleDB.cursor()


############################ MongoDB Connection
source = pymongo.MongoClient()
mydb = source["sample_db"]
mycol_act = mydb["sample_collection"]


############################
columns = (
    "col1",
    "col2",
    "col3",
    "col4",
    "col5",
    "col6",
    "col7",
    "col8",
    "col9",
    "col10",
    "col11",
    "col12",
    "col13",
    "col14",
    "col15",
    "col16",
    "col17",
)


def build_insert_query(table_name, hint=""):
    column_list = ", ".join(columns)
    placeholders = ", ".join(
        f":{i}" for i in range(1, len(columns) + 1)
    )

    return f"""
        INSERT {hint} INTO {table_name}
        ({column_list})
        VALUES ({placeholders})
    """

main_insert_query = build_insert_query(table_name="schema_name.table_name")
stage_insert_query = build_insert_query(table_name="schema_name.stg_table_name",hint="/*+ APPEND */")

batch_size = 100000


############################ Check Is running
check_is_running = "select nvl(is_running,0) from mongodb.running_etl_table  where upper(table_name) = 'TABLE_NAME' "
oracle_cursor.execute(check_is_running)
for row in oracle_cursor.fetchall():
        is_running = row[0]



if is_running == 0:

    try:
        update_query = """update mongodb.running_etl_table 
                          set is_running =  1
                        where upper(table_name) = 'TABLE_NAME' 
                       """
        oracle_cursor.execute(update_query)
        oracleDB.commit()


        mindate_query = "select nvl(max(time), 1787663804907), sysdate\
                            from mongodb.TABLE_NAME "
        oracle_cursor.execute(mindate_query)
        for row in oracle_cursor.fetchall():
            min_date = float(row[0])
            etl_time = row[1]


        maxdate_query = [
            {
        "$group": {
            "_id": None,
            "maxdate": {
        "$max": "$createtime"
                       }
                  }
            }
        ]
        maxdate_result = list(mycol_act.aggregate(maxdate_query))
        max_date = maxdate_result[0]["maxdate"]
        max_date = float(max_date)



        print(f'-------------------------New ETL Started at {etl_time}, from:{min_date}, to:{max_date}')
        oracle_cursor.execute("truncate table schema_name.stg_table_name")
        refund_query = mycol_act.find({"createtime": {"$gt": min_date, "$lte": max_date}}, batch_size=batch_size)


        counter = 0
        rows = []
        for doc in refund_query:
            id = str(doc['_id'])
            amt = None if doc.get('amt') is None or str(doc.get('amt')).lower() == "nan" else int(doc.get('amt'))
            createtime = float(doc['createtime'])
            gregorian_createtime = str(datetime.datetime.fromtimestamp(float(doc["createtime"]) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y'))
            modiftime = None if doc.get('modiftime') is None or str(doc.get('modiftime')).lower() == "nan" else int(doc.get('modiftime'))
            gregorian_modiftime = None if not doc.get("modiftime") or str(doc.get("modiftime")).lower() == "nan" else str(datetime.datetime.fromtimestamp(float(doc["modiftime"]) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y'))
            amt2 = None if doc.get('amt2') is None or str(doc.get('amt2')).lower() == "nan" else int(doc.get('amt2'))
            currentstate = None if doc.get('currentstate') is None or str(doc.get('currentState')).lower() == "nan" else int(doc.get('currentState'))
            description = '' if doc.get('description') is None or str(doc.get('description')).lower() == 'nan' else str(doc.get('description'))
            purcode = '' if doc.get('purcode') is None or str(doc.get('purcode')).lower() == 'nan' else str(doc.get('purcode'))
            rfcode = '' if doc.get('rfcode') is None or str(doc.get('rfcode')).lower() == 'nan' else str(doc.get('rfcode'))
            status = None if doc.get('status') is None or str(doc.get('status')).lower() == "nan" else int(doc.get('status'))
            trccode = '' if doc.get('trccode') is None or str(doc.get('trccode')).lower() == 'nan' else str(doc.get('trccode'))
            trfcode = '' if doc.get('trfcode') is None or str(doc.get('trfcode')).lower() == 'nan' else str(doc.get('trfcode'))
            trfdebitcode = '' if doc.get('trfdebitcode') is None or str(doc.get('trfdebitcode')).lower() == 'nan' else str(doc.get('trfdebitcode'))
            trftrc = '' if doc.get('trftrc') is None or str(doc.get('trftrc')).lower() == 'nan' else str(doc.get('trftrc'))
            amt3 = None if doc.get('amt3') is None or str(doc.get('amt3')).lower() == "nan" else int(doc.get('amt3'))

            rows.append((id, amt, createtime, gregorian_createtime,modiftime, gregorian_modiftime, amt2, currentstate,
                         description, purcode, rfcode, status, trccode, trfcode, trfdebitcode, trftrc, amt3
                         ))

            counter += 1
            if counter % batch_size == 0:
                oracle_cursor.executemany(main_insert_query, rows)
                rows = []

        if rows:
            oracle_cursor.executemany(main_insert_query, rows)
        print('Ins New Data Finished :)')



        print('CDC Started!')
        refund_cdc_query = mycol_act.find({"changetime": {"$gt": min_date, "$lte": max_date}, "createtime": {"$lte": min_date}}, batch_size=batch_size)

        counter = 0
        rows_cdc = []
        for doc in refund_cdc_query:
            id = str(doc['_id'])
            amt = None if doc.get('amt') is None or str(doc.get('amt')).lower() == "nan" else int(doc.get('amt'))
            createtime = float(doc['createtime'])
            gregorian_createtime = str(datetime.datetime.fromtimestamp(float(doc["createtime"]) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y'))
            modiftime = None if doc.get('modiftime') is None or str(doc.get('modiftime')).lower() == "nan" else int(doc.get('modiftime'))
            gregorian_modiftime = None if not doc.get("modiftime") or str(doc.get("modiftime")).lower() == "nan" else str(datetime.datetime.fromtimestamp(float(doc["modiftime"]) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y'))
            amt2 = None if doc.get('amt2') is None or str(doc.get('amt2')).lower() == "nan" else int(doc.get('amt2'))
            currentstate = None if doc.get('currentstate') is None or str(doc.get('currentState')).lower() == "nan" else int(doc.get('currentState'))
            description = '' if doc.get('description') is None or str(doc.get('description')).lower() == 'nan' else str(doc.get('description'))
            purcode = '' if doc.get('purcode') is None or str(doc.get('purcode')).lower() == 'nan' else str(doc.get('purcode'))
            rfcode = '' if doc.get('rfcode') is None or str(doc.get('rfcode')).lower() == 'nan' else str(doc.get('rfcode'))
            status = None if doc.get('status') is None or str(doc.get('status')).lower() == "nan" else int(doc.get('status'))
            trccode = '' if doc.get('trccode') is None or str(doc.get('trccode')).lower() == 'nan' else str(doc.get('trccode'))
            trfcode = '' if doc.get('trfcode') is None or str(doc.get('trfcode')).lower() == 'nan' else str(doc.get('trfcode'))
            trfdebitcode = '' if doc.get('trfdebitcode') is None or str(doc.get('trfdebitcode')).lower() == 'nan' else str(doc.get('trfdebitcode'))
            trftrc = '' if doc.get('trftrc') is None or str(doc.get('trftrc')).lower() == 'nan' else str(doc.get('trftrc'))
            amt3 = None if doc.get('amt3') is None or str(doc.get('amt3')).lower() == "nan" else int(doc.get('amt3'))

            rows.append((id, amt, createtime, gregorian_createtime,modiftime, gregorian_modiftime, amt2, currentstate,
                         description, purcode, rfcode, status, trccode, trfcode, trfdebitcode, trftrc, amt3
                         ))

            counter += 1
            if counter % batch_size == 0:
                oracle_cursor.executemany(stage_insert_query, rows_cdc)
                rows_cdc = []

        if rows_cdc:
            oracle_cursor.executemany(stage_insert_query, rows_cdc)
        print('Ins Changed Data finished :)')


        delete_query = "delete from schema_name.table_name a where exists (select 1 from schema_name.stg_table_name f where f.aid= a.aid)"
        oracle_cursor.execute(delete_query)


        final_insert = """ insert into schema_name.table_name select * from schema_name.stg_table_name """
        oracle_cursor.execute(final_insert)


        update_query = """update max_tables
                             set g_date = to_date('{}','yyyy-mm-dd hh24:mi:ss'), 
                                 g_date_char =  to_char (to_date('{}','yyyy-mm-dd hh24:mi:ss'),'yyyymmdd') ,
                                 p_date =  to_char (to_date('{}','yyyy-mm-dd hh24:mi:ss'),'yyyy/mm/dd','nls_calendar=persian') 
                             where lower(table_name) = 'table_name' """.format(etl_time, etl_time, etl_time)
        oracle_cursor.execute(update_query)
        oracleDB.commit()


    finally:
            update_query_2 = """update mongodb.running_etl_table
                                set is_running = 0
                                where upper(table_name) = 'TABLE_NAME' """
            oracle_cursor.execute(update_query_2)
            oracleDB.commit()