import pandas as pd
import psycopg2 as p
import hashlib as hb
import uuid, os, datetime, sys

class DataUp():

    def __init__(self):
        print("welcome to DataUp software")

    def connect(user,pwd,host,port,db):
        connection = p.connect(user=user,
                               password=pwd,
                               host=host,
                               port=port,
                               database=db)
        return connection
    
    def HashFileFn(filename,size):
        BLOCKSIZE = size
        hasher = hb.md5()
        with open(filename, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        #print(hasher.hexdigest())
        hashfile=hasher.hexdigest()
        return hashfile
    
    def convertToBinaryData(filename):
        #Convert digital data to binary format
        with open(filename, 'rb') as file:
            binaryData = file.read()
        return binaryData

    
    def UpdateData(filename,formid,schema,connection):
        try:
            cursor = connection.cursor()
            record = cursor.fetchone()

            uid = uuid.uuid1()
            uid= "uuid:"+str(uid)
            update_date = datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')

            sql_update_blob_query = """ update """+schema+"""_form_info_manifest_blb t1
        set "VALUE"= %s, "_URI"=%s, "_LAST_UPDATE_DATE"=%s,"_CREATION_DATE"=%s
        from """+schema+"""_form_info_manifest_ref t2 
          inner join """+schema+"""_form_info_manifest_bin t3 on 
        t2."_DOM_AURI" = t3."_URI" where t3."UNROOTED_FILE_PATH"='"""+filename+"""' and t1."_URI" = t2."_SUB_AURI" """

            sql_update_blob_query2 = """ update """+schema+"""_form_info_manifest_bin t1
            set "CONTENT_LENGTH"= %s, "CONTENT_HASH"= %s ,"_LAST_UPDATE_DATE"=%s
            from """+schema+"""_form_info t2 where
            t1."_TOP_LEVEL_AURI"=t2."_URI" and t2."FORM_ID"='"""+formid+"""' and t1."UNROOTED_FILE_PATH"='"""+filename+"""' """

            sql_update_blob_query3 = """ update """+schema+"""_form_info_manifest_ref t1
             set "_SUB_AURI"= %s, "_LAST_UPDATE_DATE"=%s, "_CREATION_DATE"=%s
            from  """+schema+"""_form_info_manifest_bin t2 where
            t1."_DOM_AURI" = t2."_URI" and t2."UNROOTED_FILE_PATH"='"""+filename+"""' """

            sql_update_blob_query4 = """ update """+schema+"""_form_info_fileset t1 set "_LAST_UPDATE_DATE"=%s
            from """+schema+"""_form_info t2 
            where t1."_PARENT_AURI"=t2."_URI" and t2."FORM_ID"='"""+formid+"""' """

            sql_update_blob_query5 = """ update """+schema+"""_form_info set "_LAST_UPDATE_DATE"=%s where "FORM_ID"='"""+formid+"""' """

            file = convertToBinaryData(filename)
            st = os.stat(filename)
            clen = st.st_size
            txthash = HashFileFn(filename,clen)
            txthash= "md5:"+txthash
            insert_blob_tuple = file
            ##result  = cursor.execute(sql_insert_blob_query, insert_blob_tuple)
            result  = cursor.execute(sql_update_blob_query, [insert_blob_tuple,uid,update_date,update_date])
            result  = cursor.execute(sql_update_blob_query2, [clen,txthash,update_date])
            result  = cursor.execute(sql_update_blob_query3, [uid,update_date,update_date])
            result  = cursor.execute(sql_update_blob_query4, [update_date])
            result  = cursor.execute(sql_update_blob_query5, [update_date])

            connection.commit()
            count = cursor.rowcount
            print("successfully updated ",count," file")
        except(Exception,p.Error) as error:
            print("Error: ",error)
        finally:
            if(connection):
                cursor.close()
                connection.close()
                print("Postgresql conn closed")
