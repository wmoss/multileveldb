import socket
from struct import pack, unpack
from request_palm import PutRequest, GetRequest, LookupRequest, AddIndex, MULTI_LEVELDB_INDEX, MULTI_LEVELDB_PUT, MULTI_LEVELDB_LOOKUP, QueryResponse, MULTI_LEVELDB_SCAN, ScanRequest
import msgpack

s = socket.socket()
s.connect(('localhost', 4455))

def send(c, p):
    p = p.dumps()
    s.send(pack('ii', c, len(p)) + p)

## for _ in xrange(10):
##     doc = BSON.encode({'cat': 'mouse',
##                        'index' : str(i)})
##     put = PutRequest(value=doc).dumps()
##     send(MULTI_LEVELDB_PUT, put)
##     print repr(s.recv(100))

## get = LookupRequest(query=doc).dumps()
## s.send(pack('ii', 3, len(get)) + get)
## resp = s.recv(200)
## unpack('ii', resp[:8])
## print map(lambda x: BSON(x).decode(), LookupResponse(resp[8:]).results)



send(MULTI_LEVELDB_INDEX, AddIndex(field=msgpack.dumps(['cat'])))
print s.recv(200)

for i in xrange(10):
    doc = msgpack.dumps({'cat': 'mouse',
                         'index' : str(i)})
    put = PutRequest(value=doc)
    send(MULTI_LEVELDB_PUT, put)
    s.recv(100)

for i in xrange(10):
    doc = msgpack.dumps({'cat': 'dog',
                         'index' : str(i)})
    put = PutRequest(value=doc)
    send(MULTI_LEVELDB_PUT, put)
    s.recv(100)


send(MULTI_LEVELDB_LOOKUP, LookupRequest(query=msgpack.dumps({'cat': 'dog'})))
typ, size = unpack('ii', s.recv(8))
if size:
    resp = QueryResponse(s.recv(size)).results
    print len(resp)
    for r in resp:
        print msgpack.loads(r)
else:
    print "FAILED"

send(MULTI_LEVELDB_SCAN, ScanRequest(query=msgpack.dumps({'cat': 'dog'})))
typ, size = unpack('ii', s.recv(8))
if size:
    resp = QueryResponse(s.recv(size)).results
    print len(resp)
    for r in resp:
        print msgpack.loads(r)
else:
    print "FAILED"
