### Basic Usage ###
from requests import post

# Access through API
TOKEN = '1BC03A7D937933D4FF32B48A625DD607'
URL = 'https://redcap.uhhospitals.org/redcap/api/'

# Receiving data from URL
payload = {'token': TOKEN, 'format': 'json', 'content': 'metadata'}

# Form-encoded data
response = post(URL, data=payload)
#print response.status_code

# Data in json format
metadata = response.json()

# Read out data
print "This project has %d fields" % len(metadata)
print
print "field_name (type) ---> field_label"
print "---------------------------"
for field in metadata:
    print "%s (%s) ---> %s" % (field['field_name'], field['field_type'], field['field_label'])
print
print 'Every field has these keys: %s' % ', '.join(sorted(metadata[0].keys()))#!/usr/bin/env python

