#! /usr/bin/env python
# License: http://creativecommons.org/publicdomain/zero/1.0/
# See http://preshing.com/20130115/view-your-filesystem-history-using-python
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
parser.add_option('-g', action='store', type='long', dest='secs', default=10,
                  help='set threshold for grouping files')
parser.add_option('-f', action='append', type='string', dest='exc_files', default=[],
                  help='exclude files matching a wildcard pattern')
parser.add_option('-d', action='append', type='string', dest='exc_dirs', default=[],
                  help='exclude directories matching a wildcard pattern')
parser.add_option('-t', action='store', type='long', dest='time', default=[],
                  help='period of time to start over')
parser.add_option('-k', action='store', type='string', dest='key', default=[],
                  help='period of time to start over')
parser.add_option('-s', action='store', type='string', dest='secret', default=[],
                  help='period of time to start over')
parser.add_option('-b', action='store', type='string', dest='bucket', default=[],
                  help='period of time to start over')


options, roots = parser.parse_args()
print  options.key
print  options.secret
print  options.bucket

if len(roots) == 0:
    print('You must specify at least one path.\n')
    parser.print_help()

def iterFiles(options, roots):
    """ A generator to enumerate the contents directories recursively. """
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            name = os.path.split(dirpath)[1]
            if any(fnmatch.fnmatch(name, w) for w in options.exc_dirs):
                del dirnames[:]  # Don't recurse here
                continue
            #stat = os.stat(os.path.normpath(dirpath))
            #yield stat.st_mtime, '', dirpath  # Yield directory
            for fn in filenames:
                if any(fnmatch.fnmatch(fn, w) for w in options.exc_files):
                    continue
                path = os.path.join(dirpath, fn)
                stat = os.lstat(os.path.normpath(path))  # lstat fails on some files without normpath
                mtime = max(stat.st_mtime, stat.st_ctime)
                yield mtime, stat.st_size, path  # Yield file

# Build file list, sort it and dump output
ptime = 0

sqlite_file = 'db3.sqlite'
file = 'file'
modify = 'modify'
size1 = 'size'
file_type = 'VARCHAR'
modify_type = 'VARCHAR'
size_type = 'VARCHAR'
table_name1 = 'full'


date = datetime.datetime.now().strftime("%y%m%d%H%M%S")
#print "Or like this: " ,datetime.datetime.now().strftime("%y-%m-%d-%H-%M")

my_file = "/root/db3.sqlite"
print "Checking if database exists"
if not os.path.isfile(my_file):
        print "Database doesnt exist, database db3.sqlite created"
        conn = sqlite3.connect(my_file)
        c = conn.cursor()
        start_time = datetime.datetime.now().strftime("%S%f")
# Creating a new SQLite table with 1 column
        print "Creating table into SQLLITE"
        c.execute('CREATE TABLE {full}{date} ({file} {file_type}, {modify} {modify_type}, {size} {size_type})'.format(full=table_name1, file=file, file_type=file_type,modify=modify,modify_type=modify_type,size=size1,size_type=size_type,date=date))
        print "Saving files into SQLLITE"
        end_time = datetime.datetime.now().strftime("%S%f")
        start_time = int(start_time)
        end_time =  int(end_time)
        duration_create_table = '{}'.format(end_time - start_time)
        print "Scan files and insert into database"
        start_time = datetime.datetime.now().strftime("%S%f")
        for mtime, size, path in sorted(iterFiles(options, roots), reverse=True):
                if ptime - mtime >= options.secs:
                        print('-' * 30)
                timeStr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
                #print('%s %10s %s' % (timeStr, size, path))
                c.execute("INSERT INTO {full}{date} ({file},{modify},{size}) VALUES ('{path}','{size2}','{mtime}')".format(full=table_name1, file=file, file_type=file_type, modify=modify, modify_type=modify_type, size=size1, size_type=size_type,path=path,mtime=mtime,size2=size,date=date))
                conn.commit()
        end_time = datetime.datetime.now().strftime("%S%f")
        start_time = int(start_time)
        end_time =  int(end_time)
        #print start_time
        #print end_time
        duration_insert_table = '{}'.format(end_time - start_time)
        print duration_insert_table


        print "Getting list files"
        start_time = datetime.datetime.now().strftime("%S%f")
        c.execute('SELECT * FROM {full}{date}'  .\
    format(full=table_name1,date=date))
        all_rows = c.fetchall()
        file = open("testfile.txt","w")
        for row in all_rows:
          file.write(row[0] + '\n')
        file.close()
        conn.close()
        ptime = mtime

        end_time = datetime.datetime.now().strftime("%S%f")
        start_time = int(start_time)
        end_time =  int(end_time)
        duration_list_file= '{}'.format(end_time - start_time)
        print duration_list_file



        print "Creating FIFO and executing Tar"
        start_time = datetime.datetime.now().strftime("%S%f")
        filekey = "{0}{1}".format(table_name1,date)
        mkfifo = "mkfifo {0}".format(filekey)
        tarcommand = "tar -cvf {0} -T testfile.txt >/dev/null 2>&1 &".format(filekey)
        removepipe = "rm -f {0}".format(filekey)
        print "Criando Named Pipe"
        sleep="sleep 10"
        os.system(mkfifo)
        print "Creating tar file"
        os.system(tarcommand)
        end_time = datetime.datetime.now().strftime("%S%f")
        start_time = int(start_time)
        end_time =  int(end_time)
        duration_tar= '{}'.format(end_time - start_time)
        print duration_tar

        print "Reading FIFO and sending file"
        #start_time = datetime.datetime.now().strftime("%S%f")

        print("Opening FIFO...")
        with open(filekey) as fifo:
           print("FIFO opened")
           while True:
               data = fifo.read()
               if len(data) == 0:
                 print("Writer closed")
                 break
               start_time = datetime.datetime.now().strftime("%S%f")

               #bucket_name = 'totino'
               s3 = boto3.client('s3',aws_access_key_id=options.key,aws_secret_access_key=options.secret,verify=False,endpoint_url='https://localhost')
               s3.put_object( Bucket=options.bucket,Body=data,Key=filekey )
               end_time = datetime.datetime.now().strftime("%S%f")
               start_time = int(start_time)
               end_time =  int(end_time)
               print start_time
               print end_time
               duration_upload= '{}'.format(end_time - start_time)
               print duration_upload
               print "Checking MD5 to check"
               start_time = datetime.datetime.now().strftime("%S%f")
               m = hashlib.md5()
               m.update(data)
               hashmd5 =  m.hexdigest()
               md5sum = s3.head_object(
               Bucket=options.bucket,
               Key=filekey
        )['ETag'][1:-1]

               if md5sum == hashmd5:
                  print "File uploaded sucessfully confirmed by md5"
               else:
                  print "File uploaded is not confirmed by md5"
               os.system(removepipe)
               end_time = datetime.datetime.now().strftime("%S%f")
               start_time = int(start_time)
               end_time =  int(end_time)
               duration_check_md5= '{}'.format(end_time - start_time)
               print duration_check_md5
               items = []
        for row in all_rows:
         item = [row[0],row[1],row[2]]
         items.append(item)
         basic_data = { "complete": 1, "perf": { "scale": 100000, "Create/Insert files names into table": duration_insert_table, "Create files name list": duration_list_file, "Execute tar/FIFO": duration_tar, "Reading FIFO/upload file to storreduce": duration_upload, "Generate md5 and check against storreduce md5": duration_check_md5 },"cpu": {"min": 19,"max": 19,"total": 19,"count": 1,"current": 19},"mem": {"min": 214564864,"max": 214564864,"total": 214564864,"count":1,"current": 214564864}, "table": {"title": "Backup List files","header": ["File Name", "Size", "Modification"], "rows": items,"caption": "File names,size,modification contained in a tar backup"}}
        print(json.dumps(basic_data))
else:
          type='type'
          table_master='sqlite_master'
          print "Database db3.sqlite exists"


          print "Checking if table full exists"
          conn = sqlite3.connect(sqlite_file)
          c = conn.cursor()
#         c.execute('SELECT name FROM {tn} WHERE {type}="table" ORDER BY Name'.\
          c.execute('SELECT name FROM {tn} WHERE {type}="table" and name LIKE "full%"'.\
    format(tn=table_master, type=type))
          all_rows = c.fetchall()
          for row in all_rows:
               full_var=row[0]
          conn.close()
          if not all_rows:
            print "table full doesn't exists"
          else:
            print "table full exists, checking diff tables"
            conn = sqlite3.connect(sqlite_file)
            c = conn.cursor()
            c.execute('SELECT name FROM {tn} WHERE {type}="table" and name LIKE "diff%"'.\
    format(tn=table_master, type=type))
            all_rows = c.fetchall()
            #print all_rows
            lenght = len(all_rows)
            if lenght >= options.time:
             print "existem mais 5 backups diff"
             rm = "mv {0} {0}{1}.bkp".format(my_file,date)
             filekey = "{0}{1}".format(table_name1,date)
             os.system(rm)
             print "Creating new database"
             conn = sqlite3.connect(my_file)
             c = conn.cursor()
             print "Creating table full into SQLLITE"
             #print date
             c.execute('CREATE TABLE full{date} ({file} {file_type}, {modify} {modify_type}, {size} {size_type})'.format(full=full_var, file=file, file_type=file_type,modify=modify,modify_type=modify_type,size=size1,size_type=size_type,date=date))
             print "Saving files into SQLLITE"
             c.execute('SELECT name FROM {tn} WHERE {type}="table" and name LIKE "full%"'.\
    format(tn=table_master, type=type))
             all_rows = c.fetchall()
             for row in all_rows:
               full_var=row[0]

             for mtime, size, path in sorted(iterFiles(options, roots), reverse=True):
                if ptime - mtime >= options.secs:
                        print('-' * 30)
                timeStr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
                #print('%s %10s %s' % (timeStr, size, path))
                c.execute("INSERT INTO {full} ({file},{modify},{size}) VALUES ('{path}','{size2}','{mtime}')".format(full=full_var, file=file, file_type=file_type, modify=modify, modify_type=modify_type, size=size1, size_type=size_type,path=path,mtime=mtime,size2=size,date=date))
                conn.commit()
             c.execute('SELECT * FROM {full}{date}'  .\
    format(full=table_name1,date=date))
             all_rows = c.fetchall()
             #print all_rows
             file = open("testfile.txt","w")
             for row in all_rows:
                        file.write(row[0] + '\n')
             file.close()
             conn.close()
             ptime = mtime
             mkfifo = "mkfifo {0}".format(filekey)
             tarcommand = "tar -cvf {0} -T testfile.txt &".format(filekey)
             removepipe = "rm -f {0}".format(filekey)
             print "Criando Named Pipe"
             os.system(mkfifo)
             print "Creating tar file"
             os.system(tarcommand)
             print "Uploading tar file"
             #os.system(uploadtar)
             with open(filekey) as fifo:
               print("FIFO opened")
               while True:
                 data = fifo.read()
                 if len(data) == 0:
                   break
                 #bucket_name = 'totino'
                 #print data
                 s3 = boto3.client('s3',aws_access_key_id=options.key,aws_secret_access_key=options.secret,verify=False,endpoint_url='https://localhost')
                 s3.put_object( Bucket=options.bucket,Body=data,Key=filekey )
                 m = hashlib.md5()
                 m.update(data)
                 hashmd5 =  m.hexdigest()
                 md5sum = s3.head_object(
                 Bucket=options.bucket,
                 Key=filekey
             )['ETag'][1:-1]
                 if md5sum == hashmd5:
                   print "File uploaded sucessfully confirmed by md5"
                 else:
                   print "File uploaded is not confirmed by md5"

             print "Removing pipe"
             os.system(removepipe)
#else:
            else:
             print "nao existem 5 backups diff, criando backup diff"
             conn = sqlite3.connect(sqlite_file)
             c = conn.cursor()
             lenght = lenght + 1
             c.execute('CREATE TABLE diff{date} ({file} {file_type}, {modify} {modify_type}, {size} {size_type})'.format(full=table_name1, file=file, file_type=file_type,modify=modify,modify_type=modify_type,size=size1,size_type=size_type,lenght=lenght,date=date))
             for mtime, size, path in sorted(iterFiles(options, roots), reverse=True):
                if ptime - mtime >= options.secs:
                        print('-' * 30)
                timeStr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
                #print('%s %10s %s' % (timeStr, size, path))
                c.execute("INSERT INTO diff{date} ({file},{modify},{size}) VALUES ('{path}','{size2}','{mtime}')".format(full=table_name1, file=file, file_type=file_type, modify=modify, modify_type=modify_type, size=size1, size_type=size_type,path=path,mtime=mtime,size2=size,lenght=lenght,date=date))
                conn.commit()
                all_rows = c.fetchall()
             c.execute('SELECT * FROM {full} EXCEPT Select * from diff{date} UNION ALL select * from diff{date} EXCEPT select * from {full}'  .\
    format(type=type,lenght=lenght,date=date,full=full_var))
             all_rows = c.fetchall()
             file = open("testfile.txt","w")
             #print all_rows
             for row in all_rows:
                #print(row[0])
                file.write(row[0] + '\n')
             file.close()
             ptime = mtime
             conn.close()
             filekey="diff{0}".format(date)
             mkfifo = "mkfifo {0}".format(filekey)
             tarcommand = "tar -cvf {0} -T testfile.txt &".format(filekey)
             removepipe = "rm -f {0}".format(filekey)
             #uploadtar = "cat diff{0} > diff{0}.tar.gz".format(date)
             print "Criando Named Pipe"
             os.system(mkfifo)
             print "Creating tar file"
             os.system(tarcommand)
             with open(filekey) as fifo:
               print("FIFO opened")
               while True:
                 data = fifo.read()
                 if len(data) == 0:
                   print("Writer closed")
                   break
#       sys.stdout = open('file', 'w')
               #print(data)

                 #bucket_name = 'totino'
                 s3 = boto3.client('s3',aws_access_key_id=options.key,aws_secret_access_key=options.secret,verify=False,endpoint_url='https://localhost')
                 s3.put_object( Bucket=options.bucket,Body=data,Key=filekey )
                 m = hashlib.md5()
                 m.update(data)
                 hashmd5 =  m.hexdigest()
                 md5sum = s3.head_object(
                 Bucket=options.bucket,
                 Key=filekey
                 )['ETag'][1:-1]
                 if md5sum == hashmd5:
                   print "File uploaded sucessfully confirmed by md5"
                 else:
                   print "File uploaded is not confirmed by md5"

                 print "Removing pipe"
                 os.system(removepipe)
