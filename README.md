MultiLevelDB is a multi-index schemaless database built on LevelDB. It stores data in the <a href="http://www.msgpack.org">Message Pack</a> format.

# Protocol

The protocol is implemented using protocol buffers (full details in proto/request.proto) and a very simple wire format. The wire format is:

[ 4 bytes message type ] [ 4 byte size ] [ protcol buffer ]


# Technical Details

## Namespaces

The key prefexes map as follows:

* [0] => config
* [1] => User data
* [2] => indexes
* [3] => free list

## Indexes

Indexes are stored in three keys, dictated below. To save space, we create a mapping from the bytes of the index field to an integer.

* [2][index field][0] => [index id]
* [2][index id][0] => [index field]
* [2][index id][field value][primary id] => [0] (value ignored)
