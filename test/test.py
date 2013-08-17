from pymultileveldb import MultiLevelDBClient
import py

class Tests(object):
    def __init__(self):
        self.client = MultiLevelDBClient()

    def test_put_get(self):
        doc = {'cat': 'dog'}
        self.client.put(doc)
