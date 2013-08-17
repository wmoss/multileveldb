import socket
from struct import pack, unpack
from request_palm import PutRequest, GetRequest, LookupRequest, QueryResponse, MULTI_LEVELDB_PUT, MULTI_LEVELDB_GET, MULTI_LEVELDB_LOOKUP
from bson import BSON

s = socket.socket()
s.connect(('localhost', 4455))

def send(c, p):
    p = p.dumps()
    s.send(pack('ii', c, len(p)) + p)

for i in xrange(10):
    doc = BSON.encode({'cat': 'mouse',
                       'index' : str(i)})
    put = PutRequest(value=doc)
    send(MULTI_LEVELDB_PUT, put)
    print repr(s.recv(100))

send(MULTI_LEVELDB_LOOKUP, LookupRequest(query=BSON.encode({'cat': 'mouse'})))
resp = s.recv(2000)
unpack('ii', resp[:8])
import pprint
pprint.pprint(map(lambda x: BSON(x).decode(), QueryResponse(resp[8:]).results))
