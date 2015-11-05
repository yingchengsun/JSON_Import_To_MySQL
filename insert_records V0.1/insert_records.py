# -*- coding: utf-8 -*-
'''
Created on 09-29-2015
@author: Yingcheng Sun

'''
import MySQLdb
import json

try:
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='1234',port=3306)
    cur=conn.cursor()
    
    cur.execute('create database if not exists python')
    conn.select_db('python')
    cur.execute('create table if not exists patient (city_of_birth varchar(50), first_name varchar(20), last_name varchar(20),\
    date_of_birth date, gender varchar(20), email varchar(50), phone varchar(20), street varchar(50), \
    postal_code varchar(20), country varchar(20), record_id int (20) not null primary key ,demographics_complete varchar(20), ethnicity varchar(50))\
    DEFAULT CHARSET=utf8 ')
   
    fileHandle=file('data.jo')
    fileList = fileHandle.readlines()
    dictinfo = json.loads(fileList[0])
    
    for patient_record in dictinfo:
        record_list=[]
        for record_value in patient_record.values():
            #transfer the coding type 'unicode' to utf8
            if (type(record_value)==unicode):
                record_value=record_value.encode('utf-8');
            record_list.append(record_value)
        #print len(record_list)
        cur.execute('insert into patient values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',record_list)
        conn.commit()
    cur.close()
    conn.close()
    print len(dictinfo),'records have been inserted successfully!'
    
except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])


