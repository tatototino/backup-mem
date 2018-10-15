# USING python mysql.py -i xxxxxxxx -d 'xxxxxxx' -p 'xxxxxxxxxx' -u root -k 'xxxxxxxx' -s 'xxxxxxxx' -b 'xxxxxxxxxx'

#! /usr/bin/env python
import optparse
import fnmatch
import time
import sqlite3
import pathlib
import pdb
import string
import time
import datetime
import errno
import sys
import boto3
import botocore
import os
import hashlib
import json
import re
#import string


# Parse options
parser = optparse.OptionParser(usage='Usage: %prog [options] path [path2 ...]')
parser.add_option('-i', action='store', type='string', dest='hostname', default='127.0.0.1',help='hostname')
parser.add_option('-d', action='store', type='string', dest='database', default=[],help='exclude files matching a wildcard pattern')
parser.add_option('-u', action='store', type='string', dest='username', default=[],help='period of time to start over')
parser.add_option('-p', action='store', type='string', dest='password', default=[],help='period of time to start over')
parser.add_option('-k', action='store', type='string', dest='key', default=[],
                  help='period of time to start over')
parser.add_option('-s', action='store', type='string', dest='secret', default=[],
                  help='period of time to start over')
parser.add_option('-b', action='store', type='string', dest='bucket', default=[],
                  help='period of time to start over')


options, roots = parser.parse_args()
print  options.username
print  options.password
print  options.hostname
print  options.database
print  options.key
print  options.secret
print  options.bucket


print "Creating FIFO and executing Tar"
start_time = datetime.datetime.now().strftime("%S%f")
mkfifo = "mkfifo {0}".format(options.database)
tarcommand = "mysqldump -u {2} -p{1} -h{0} {3}    | bzip2 > {3} & 2>&1 &".format(options.hostname,options.password,options.username,options.database)
print tarcommand
removepipe = "rm -f {0}".format(options.database)
#print "Criando Named Pipe"
#sleep="sleep 10"
os.system(mkfifo)
print "Creating dump"
os.system(tarcommand)
end_time = datetime.datetime.now().strftime("%S%f")
#start_time = int(start_time)
print "Reading FIFO and sending file"
print("Opening FIFO...")
with open(options.database) as fifo:
  print("FIFO opened")
  while True:
    data = fifo.read()
    if len(data) == 0:
      print("Writer closed")
      break
    start_time = datetime.datetime.now().strftime("%S%f")
    s3 = boto3.client('s3',aws_access_key_id=options.key,aws_secret_access_key=options.secret,verify=False,endpoint_url='https://storreduce')
    s3.put_object( Bucket=options.bucket,Body=data,Key=options.database )
    start_time = int(start_time)
    end_time =  int(end_time)
    duration_upload= '{}'.format(end_time - start_time)
    print "Checking MD5 to check"
    start_time = datetime.datetime.now().strftime("%S%f")
    m = hashlib.md5()
    m.update(data)
    hashmd5 =  m.hexdigest()
    md5sum = s3.head_object(Bucket=options.bucket,Key=options.database)['ETag'][1:-1]
    if md5sum == hashmd5:
       print "File uploaded sucessfully confirmed by md5"
    else:
       print "File uploaded is not confirmed by md5"
    os.system(removepipe)
    end_time = datetime.datetime.now().strftime("%S%f")
    start_time = int(start_time)
    end_time =  int(end_time)
    duration_check_md5= '{}'.format(end_time - start_time)
items = []
print "Dump realizado com sucesso"
#for row in all_rows:
#  item = [row[0],row[1],row[2]]
#  items.append(item)
#  basic_data = { "complete": 1, "perf": { "scale": 100000, "Create/Insert files names into table": duration_insert_table, "Create files name list": duration_list_file, "Execute tar/FIFO": duration_tar, "Reading FIFO/upload file to storreduce": duration_upload, "Generate md5 and check against storreduce md5": duration_check_md5 },"cpu": {"min": 19,"max": 19,"total": 19,"count": 1,"current": 19},"mem": {"min": 214564864,"max": 214564864,"total": 214564864,"count":1,"current": 214564864}, "table": {"title": "Backup List files","header": ["File Name", "Size", "Modification"], "rows": items,"caption": "File names,size,modification contained in a tar backup"}}
#print(json.dumps(basic_data))
