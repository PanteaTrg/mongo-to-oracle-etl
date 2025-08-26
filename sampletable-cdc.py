import datetime
from bson import ObjectId
import pytz
import pymongo
import cx_Oracle
import  jdatetime
from cryptography.fernet import Fernet
import base64


try:

    def flatten_data(y):
        out = {}
        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                lst=[]
                i = 0
                for a in x:
                    flatten(a,name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
        flatten(y)
        return out


    ############################ Oracle Connection
    connection = cx_Oracle.connect()
    cursor = connection.cursor()

    ############################ MongoDB Connection
    myclient = pymongo.MongoClient()
    mydb = myclient["sampledb"]
    mycoll = mydb["samplecollection"]


    ############################ Check if ETL is running
    check_isrun = " select nvl(is_running,0) from table_of_running_etls where upper(table_name) = 'sampletable' "
    cursor.execute(check_isrun)
    for row in cursor.fetchall():
        is_running = row[0]


    if is_running == 0:
        update = """update table_of_running_etls \
                          set is_running = 1\
                          where upper(table_name) = 'sampletable' """
        cursor.execute(update)
        connection.commit()


        from_date_q = "select nvl(max(creationdate), 1575812015492) from sampletable "
        cursor.execute(from_date_q)
        for row in cursor.fetchall():
            from_date = int(row[0])


        to_date_q = "select (sysdate - to_date(to_char(from_tz(cast(to_date('1970-01-01 00',\
                                                                'yyyy-mm-dd hh24:mi') as\
                                                        timestamp),\
                                                   'UTC') at time zone 'Iran',\
                                           'yyyy-mm-dd hh24:mi'),\
                                   'yyyy-mm-dd hh24:mi') - (case\
                  when to_char(sysdate, 'yyyy', 'nls_calendar=persian') not in\
                       ('1402', '1403', '1404') and\
                       to_char(sysdate, 'mm', 'nls_calendar=persian') between '01' and '06' and\
                       to_char(sysdate, 'mmdd', 'nls_calendar=persian') not in\
                       ('0101', '0102', '0631') then\
                   1 / 24\
                  else\
                   0\
                end)) * 1000 * 24 * 60 * 60 - 300000\
           from dual"
        cursor.execute(to_date_q)
        for row in cursor.fetchall():
            to_date = int(row[0])


        print('*********************************************** NEW ******************************************************')

        insert_query = """INSERT INTO sampletable (ID, CUSTOMERNUMBER, CUSTOMERNATIONALCODE, CARDNUMBER, INITIALBALANCE,JD_DATE,GD_DATE) 
                          VALUES (:1, :2, :3, :4, :5)"""

        fields = {
            "_id": 1,
            "customerNumber": 1,
            "customerNationalCode": 1,
            "cardNumber": 1,
            "initialBalance": 1,
            "date":1
        }

        batch_size = 10000
        counter = 0

        rows_to_insert_main = []
        mongo_query = mycoll.find({"Date": {"$gt": from_date, "$lte": to_date}},projection=fields,batch_size=batch_size)
        count = mycoll.count_documents({"Date": {"$gt": from_date, "$lte": to_date}})
        print(f'Matching documents count:{count}, Insert main data, from_date: {from_date}, to_date: {to_date} start!')

        for i in mongo_query:
            js = flatten_data(i)
            ID = str(js['_id'])
            CUSTOMERNUMBER = js.get('customerNumber', '')
            CUSTOMERNATIONALCODE = js.get('customerNationalCode', '')
            CARDNUMBER = js.get('cardNumber', '')
            INITIALBALANCE = js['initialBalance']
            DATE = js.get('creationDate')
            JD_DATE = None if 'date' not in js.keys() or str('date') == 'nan' else jdatetime.datetime.fromtimestamp(float(DATE) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%Y%m%d')
            GD_DATE = None if 'date' not in js.keys() or str('date') == 'nan' else str(datetime.datetime.fromtimestamp(float(DATE) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y %I:%M:%S%p'))

            rows_to_insert_main.append((ID,CUSTOMERNUMBER,CUSTOMERNATIONALCODE,CARDNUMBER,INITIALBALANCE,JD_DATE,GD_DATE))

            counter += 1
            if counter % batch_size == 0:
                cursor.executemany(insert_query, rows_to_insert_main)
                # print(f"Inserted {counter} documents so far")
                rows_to_insert_main = []
        if rows_to_insert_main:
            cursor.executemany(insert_query, rows_to_insert_main)
            print(f"Inserted total {counter} documents")


        connection.commit()
        print("Insert main data finished!")


        print('---------------- *CDC* -----------------')

        def chunked(seq, n):
            for j in range(0, len(seq), n):
                yield seq[j:j + n]

        def delete_ids_from_sampletable(cursor, ids, max_in=900):
            for chunk in chunked(ids, max_in):
                placeholders = ",".join([f":{i + 1}" for i in range(len(chunk))])
                delete_sql = f"DELETE FROM sampletable WHERE id IN ({placeholders})"
                cursor.execute(delete_sql, chunk)

        def flush_batch():
            delete_ids_from_sampletable(cursor, ids_to_delete_cdc, max_in=900)
            cursor.executemany(insert_query, rows_to_insert_cdc)
            ids_to_delete_cdc.clear()
            rows_to_insert_cdc.clear()


        id_cdc_min = "SELECT cdc_id cdc_id_min, sysdate up_date from table_of_dates  where upper(table_name) = 'sampletable' "
        cursor.execute(id_cdc_min)
        for row in cursor.fetchall():
             cdc_id_min = row[0] + 1
             up_date = row[1]

        cursor.execute(
            "SELECT nvl(max(iid),0) \
            from table_of_cdc b \
            where exists (select * from sampletable a where a.id = b.id) and b.deleted = 0 ")
        cdc_id_max = cursor.fetchone()[0]

        cursor.execute(
            "select distinct id \
             from table_of_cdc \
             where iid between :1 and :2 and deleted = 0",(cdc_id_min, cdc_id_max))
        objectId = cursor.fetchall()


        lst = [ObjectId(str(i[0])) for i in objectId]
        data = mycoll.find({"_id":{"$in":lst}},projection=fields,batch_size=batch_size)

        rows_to_insert_cdc = []
        ids_to_delete_cdc = []
        counter = 0

        print(f'handling changed data from cdc_id_min:{cdc_id_min} to cdc_id_max:{cdc_id_max} start!')
        for i in data:
            js = flatten_data(i)
            ID = str(js['_id'])
            CUSTOMERNUMBER = js.get('customerNumber', '')
            CUSTOMERNATIONALCODE = js.get('customerNationalCode', '')
            CARDNUMBER = js.get('cardNumber', '')
            INITIALBALANCE = js['initialBalance']
            DATE = js.get('creationDate')
            JD_DATE = None if 'date' not in js.keys() or str('date') == 'nan' else jdatetime.datetime.fromtimestamp(float(DATE) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%Y%m%d')
            GD_DATE = None if 'date' not in js.keys() or str('date') == 'nan' else str(datetime.datetime.fromtimestamp(float(DATE) / 1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y %I:%M:%S%p'))

            ids_to_delete_cdc.append(ID)
            rows_to_insert_main.append(ID, CUSTOMERNUMBER, CUSTOMERNATIONALCODE, CARDNUMBER, INITIALBALANCE, JD_DATE, GD_DATE)

            counter += 1
            if counter % batch_size == 0:
                flush_batch()
                # print(f"Processed {counter} documents so far")
        flush_batch()
        print(f"Inserted total {counter} documents")
        print('cdc: delete + insert done')



        print('Delete records start!')
        cursor.execute('Delete from sampletable where id in (select id from table_of_cdc where deleted=1) ')
        deleted = cursor.rowcount
        print(f'Delete records done!{deleted} rows deleted.')


        update_query = """update table_of_dates \
                set g_date = to_date('{}','yyyy-mm-dd hh24:mi:ss'),\
                g_date_char =  to_char (to_date('{}','yyyy-mm-dd hh24:mi:ss'),'yyyymmdd') ,\
                p_date =  to_char (to_date('{}','yyyy-mm-dd hh24:mi:ss'),'yyyy/mm/dd','nls_calendar=persian'),\
                cdc_id = {}\
                where upper(table_name) = 'sampletable' """.format(up_date,up_date,up_date,cdc_id_max)
        cursor.execute(update_query)

        update_isrunning_activations = """ update table_of_running_etls \
                                                 set is_running = 0\
                                                 where upper(table_name) = 'sampletable' \
                                       """
        cursor.execute(update_isrunning_activations)
        connection.commit()


except Exception as e:
    print("An error occurred:", str(e))
    raise
finally:
    cursor.execute("""update table_of_running_etls\
                             set is_running = 0\
                             where upper(table_name) = 'sampletable' \
                   """)
    connection.commit()
    print("is_running reset to 0")
    print('*********************************************** END ******************************************************')
    print()


connection.close()
