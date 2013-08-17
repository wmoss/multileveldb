import socket
from struct import pack, unpack
from request_palm import MULTI_LEVELDB_DUMP, QueryResponse
import msgpack

s = socket.socket()
s.connect(('localhost', 4455))

s.send(pack('ii', MULTI_LEVELDB_DUMP, 0))
typ, size = unpack('ii', s.recv(8))
if size:
    resp = QueryResponse(s.recv(size)).results
    for r in resp:
        print msgpack.loads(r)
else:
    print "FAILED"
