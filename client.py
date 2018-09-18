#!/usr/bin/env python
import socket
import optparse

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
parser.add_option('-i', action='store', type='string', dest='host', default=10,
                  help='set threshold for grouping files')
options, roots = parser.parse_args()
i=0
for root in roots:
   root=root
if len(roots) == 0:
    print('You must specify at least one path.\n')
    parser.print_help()
#print options.host
#print options.key
#print options.secret
#print options.bucket
#print options.time
#print root

TCP_IP = options.host
TCP_PORT = 5005
BUFFER_SIZE = 50000
qtd = options.time
type = 'full'
dir = root
bucket = options.bucket
key = options.key
secret = options.secret
full_text = '{0} {1} {2} {3} {4} {5}'.format(qtd,dir,type,bucket,key,secret)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))
s.send(full_text)
data = s.recv(BUFFER_SIZE)
s.close()
print data
