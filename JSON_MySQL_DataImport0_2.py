### Data Import In MySQL DB ###
#-*-coding:utf-8-*-  

import MySQLdb
import json
from collections import OrderedDict

try:
        # Access to MySQLdb
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='1234',port=3306,charset='utf8')
    cur=conn.cursor()
        # If doesn't already exist, create schema and table
    cur.execute('create database if not exists redcap')
    conn.select_db('redcap')
    cur.execute('create table if not exists test (record_id int (20) not null primary key, first_name varchar(20), last_name varchar(20),\
    gender varchar(20), date_of_birth date, city_of_birth varchar(50), ethnicity varchar(50), country varchar(50), postal_code varchar(20),\
    street varchar(50), phone varchar(20), email varchar(50), demographics_complete varchar(20) )\
    DEFAULT CHARSET=utf8 ')
    
    # Enter json-file  
    fileHandle=file('data.json')
    fileList = fileHandle.readlines()
    #OrderedDict: keep the order of dictionary data read from json same as the original order
    dictinfo = json.loads(fileList[0],object_pairs_hook=OrderedDict)
    
        # Iterate through json-file
    for test_record in dictinfo:
        record_list=[]
        for record_value in test_record.values():
        # Transfer the coding type 'unicode' to utf8 and store records in list
            if (type(record_value)==unicode):
                record_value=record_value.encode('utf-8')
            record_list.append(record_value)
        # If there are some values missed in the final line, append "null" values
        while(len(record_list)<13):
            record_list.append('')
        # Insert records in MySQL 
        cur.execute('replace into test values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',record_list)
        conn.commit()
    
    #cur.execute("SELECT record_id, city_of_birth FROM test")
    #rows = cur.fetchall ()
    #fl=open("city_of_birth.txt",'w')
    #for row in rows:
    #    fl.write(row[1]+'\n')
        #print row[1].decode('utf-8').encode("GB18030")
    #fl.close()
    
    cur.close()
    conn.close()
    fileHandle.close()
    print len(dictinfo),'records have been inserted successfully!'
    
        # If error occurs while inserting records
except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])