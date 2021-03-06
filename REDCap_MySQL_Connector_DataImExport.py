﻿#coding=utf-8 

'''
Created on 10-02-2015
@author: Bianka Marlen Hubert
Modified by Yingcheng Sun
            10-19-2015

'''

    # Load Libraries
import pycurl, cStringIO 
import json
import MySQLdb
from collections import OrderedDict

### Timecounter for program ###
from datetime import datetime
start_time = datetime.now()
    ### Data Export From REDCap ###

buf = cStringIO.StringIO()  #Set up buffer
# Access REDCap through API
data = {
    'token': '1BC03A7D937933D4FF32B48A625DD607',
    'content': 'record',
    'format': 'json',
    'type': 'flat',
   #'fields': {'first_name','gender','last_name'},
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}
ch = pycurl.Curl()  #Creates new curl object
ch.setopt(ch.URL, 'https://redcap.uhhospitals.org/redcap/api/')  #Corresponds with REDCap
ch.setopt(ch.HTTPPOST, data.items())  #Accepts a list of strings
ch.setopt(ch.WRITEFUNCTION, buf.write)  #Writes it to a buffer
ch.perform()  #Performs a file transfer
ch.close()  #Ends curl session

### Data Import In MySQL DB ###
try:
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='****',port=3306)  #Connects to MySQLdb
    cur=conn.cursor()
    
    cur.execute('create database if not exists redcap')  #Creates schema
    conn.select_db('redcap')
    cur.execute('create table if not exists test (record_id int (20) not null primary key, first_name varchar(20), last_name varchar(20),\
    gender varchar(20), date_of_birth date, city_of_birth varchar(50), ethnicity varchar(50), country varchar(50), postal_code varchar(20),\
    street varchar(50), phone varchar(20), email varchar(50), demographics_complete varchar(20) )\
    DEFAULT CHARSET=utf8 ')  #Creates table with columns
    dictinfo= json.loads(buf.getvalue(),object_pairs_hook=OrderedDict)#OrderedDict: keep the order of dictionary data read from json same as the original order
    
    for test_record in dictinfo:  #Iterates through all records
        record_list=[]
        for record_value in test_record.values():  #Iterates through values of one record
            if (type(record_value)==unicode):  #Transfer the coding type 'unicode' to utf8 and store records in list
                record_value=record_value.encode('utf-8')
            record_list.append(record_value)
        #while(len(record_list)<13):  #If there are some values missed in the final line, append "null" values
        #    record_list.append('')
        cur.execute('replace into test values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',record_list)  #Inserts records in MySQL
        conn.commit()
     
    '''
        For testing the encoding issue, values of 'city_of_birth'
        are saved to a city_of_birth.txt file
    '''
    cur.execute("SELECT record_id, city_of_birth FROM test")
    rows = cur.fetchall ()
    fl=open("city_of_birth.txt",'w')
    for row in rows:
        fl.write(row[1]+'\n')
        #print row[1].decode('utf-8').encode("GB18030")
    fl.close()
    
    cur.close()
    conn.close() #Closes connection to MySQL db
    buf.close()  #Closes buffer
    print len(dictinfo),'records have been inserted successfully!'

except MySQLdb.Error,e:  #If error occurs while inserting records
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])


### Timecounter for program ###
end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))