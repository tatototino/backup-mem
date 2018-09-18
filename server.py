#!/usr/bin/env python
import socket
import os
from subprocess import Popen,PIPE,STDOUT,call
TCP_IP = '127.0.0.1'
TCP_PORT = 5005
BUFFER_SIZE = 50000  # Normally 1024, but we want fast response
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
#conn, addr = s.accept()
#print 'Connection address:', addr
while 1:
    conn, addr = s.accept()
    data = conn.recv(BUFFER_SIZE)
    #print data
    if not data:
       print 'not data'
       break
    else:
      list_data = data.split()
      command="python list_modification -t {0} -k '{4}' -s '{5}' -b '{3}' {1}".format(list_data[0],list_data[1],list_data[2],list_data[3],list_data[4],list_data[5])
      #print command
      proc=Popen(command, shell=True, stdout=PIPE, )
      data=proc.communicate()[0]
      conn.send(data)
    #conn.close()
[root@storreduce ~]# cat server.py
#!/usr/bin/env python
import socket
import os
from subprocess import Popen,PIPE,STDOUT,call
TCP_IP = '127.0.0.1'
TCP_PORT = 5005
BUFFER_SIZE = 50000  # Normally 1024, but we want fast response
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
#conn, addr = s.accept()
#print 'Connection address:', addr
while 1:
    conn, addr = s.accept()
    data = conn.recv(BUFFER_SIZE)
    #print data
    if not data:
       print 'not data'
       break
    else:
      list_data = data.split()
      command="python list_modification -t {0} -k '{4}' -s '{5}' -b '{3}' {1}".format(list_data[0],list_data[1],list_data[2],list_data[3],list_data[4],list_data[5])
      #print command
      proc=Popen(command, shell=True, stdout=PIPE, )
      data=proc.communicate()[0]
      conn.send(data)
    #conn.close()
