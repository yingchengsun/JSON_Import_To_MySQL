#!/usr/bin/env python
'''
Created on 10-16-2015
@author: Bianka Marlen Hubert

'''

    # Load Libraries
import pycurl, cStringIO, MySQLdb 
import json
from requests import post
from collections import OrderedDict

### Timecounter for program ###
from datetime import datetime
start_time = datetime.now()


    ### Data Export From REDCap ###

## Store records in json file
REDCap_TOKEN = '1BC03A7D937933D4FF32B48A625DD607'
REDCap_URL = 'https://redcap.uhhospitals.org/redcap/api/'
buf = cStringIO.StringIO()                                              # Set up buffer
data = {                                                                # Access REDCap through API
    'token': REDCap_TOKEN,
    'content': 'record',
    'format': 'json',
    'type': 'flat',
   #'fields': {'record_id'},
   #'forms': {'demographics'},
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

ch = pycurl.Curl()                                                      # Creates new curl object

ch.setopt(ch.URL, REDCap_URL)                                           # Corresponds with REDCap

ch.setopt(ch.HTTPPOST, data.items())                                    # Accepts a list of strings

ch.setopt(ch.WRITEFUNCTION, buf.write)                                  # Writes it to a buffer

ch.perform()                                                            # Performs a file transfer

ch.close()                                                              # Ends curl session
#with open('data.json', 'w') as my_file:                                 # Creates/Writes/Saves json-file from generated buffer
#    my_file.write(buf.getvalue())                                       # Write file command
#print buf.getvalue()
#buf.close()                                                             # Closes buffer
#print 'Records have been successfully inserted in a json file!\n'
payload = {'token': REDCap_TOKEN, 'format': 'json', 'content': 'metadata'}  # Receiving data from URL
response = post(REDCap_URL, data=payload)                               # Form-encoded data
metadata = response.json()                                              # Data in json format

## Store keys
keys = []

for field in metadata:
    keys.append(str(field['field_name']))
#print '%s \n' % (keys)

## Generating 1st mysql command >create table if not exist ... ( ... varchar(50), ... varchar(50), ...)
instrument_name = str(field['form_name'])

#print instrument_name+'\n'
insert_v='create table if not exists  ('+keys[0]+' varchar(20) not null primary key, ' # Start of string/complete string
v = ' varchar(50), '                                                    # Concatenation
e_v = instrument_name+'_complete varchar(50)) DEFAULT CHARSET=utf8 '    # End of string
nr_keys = len(metadata)-1
i = 0
while nr_keys>0:
    i+=1
    insert_v=insert_v+keys[i]+str(v)
    nr_keys-=1
insert_v+=str(e_v)
#print insert_v+'\n'

## Genrating 2nd mysql command >values(%s, %s, %s, %s, %s)
insert_s='replace into test values('                                    # Start of string/complete string
s = '%s, '                                                              # Concatenation
e_s = '%s)'                                                             # End of string
nr_keys = len(metadata)
while nr_keys>0:
    insert_s+=str(s)
    nr_keys-=1
insert_s+=str(e_s)
#print insert_s+'\n'




### Data Import In MySQL DB ###

try:
        
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='1234',port=3306, charset='utf8')  # Connects to MySQLdb
    cur=conn.cursor()
    
    create_schema = 'create database if not exists redcap'
    cur.execute(create_schema)                                          # Creates schema
    #print
    conn.select_db('redcap')
    cur.execute(insert_v)                                               # Creates table with columns
    #print
    
    #fileHandle=file('data.json')                                        # Enters json-file
    #fileList = fileHandle.readlines()
    #dictinfo = json.loads(fileList[0],object_pairs_hook=OrderedDict)    # OrderedDict: keep the order of dictionary data read from json same as the original order
    dictinfo = json.loads(buf.getvalue(),object_pairs_hook=OrderedDict)

    for test_record in dictinfo:                                        # Iterates through all records
        record_list=[]
        for record_value in test_record.values():                       # Iterates through values of one record
            if (type(record_value)==unicode):                           # Transfer the coding type 'unicode' to utf8 and store records in list
                record_value=record_value.encode('utf-8')
            record_list.append(record_value)
        while(len(record_list)<13):                                     # If there are some values missed in the final line, append "null" values
            record_list.append('')
        cur.execute(insert_s,record_list)                               # Inserts records in MySQL
        conn.commit()
    cur.close()
    conn.close()                                                        # Closes connection to MySQL db
    buf.close()                                                         # Closes buffer
    print len(dictinfo),'records have been inserted successfully!'
    
except MySQLdb.Error,e:                                                 # If error occurs while inserting records
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])

#os.remove('data.json')


### Timecounter for program ###
end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))