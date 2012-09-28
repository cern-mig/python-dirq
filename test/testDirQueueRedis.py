# -*- coding: utf-8 -*-

import os
import random
import shutil
import string
import unittest
import time

from dirq.QueueRedis import QueueRedis, LOCKED_SUFFIX
import tempfile

__all__ = ['TestQueueRedis']

PATH = ''.join(random.choice(string.letters) for i in xrange(10))
redis_params = {"path": PATH}


class TestQueueRedis(unittest.TestCase):
    _qr = None

    def _clean_it(self):
        if self._qr is None:
            self._qr = QueueRedis(**redis_params)
        # cleaning
        for elem in self._qr:
            if self._qr.lock(elem):
                self._qr.remove(elem)

    def setUp(self):
        self._clean_it()

    def tearDown(self):
        self._clean_it()

    def test01init(self):
        'QueueRedis.__init__()'
        qr = QueueRedis(**redis_params)
        assert qr._path == redis_params["path"]

    def test02add(self):
        'QueueRedis.add()'
        data = 'foo bar'
        qr = QueueRedis(**redis_params)
        elem = qr.add(data)
        assert qr.get(elem) == data
        self._clean_it()

    def test03lockunlok(self):
        'QueueRedis.lock()'
        data = 'foo bar'
        qr = QueueRedis(**redis_params)
        elem = qr.add(data)
        assert qr.get(elem) == data
        self.assert_(qr.lock(elem))
        self.failUnless(qr.get("%s%s" % (elem, LOCKED_SUFFIX)) is not None)
        qr.unlock(elem)
        self.failUnless(qr.get("%s%s" % (elem, LOCKED_SUFFIX)) is None)
        self._clean_it()

    def test04get(self):
        'QueueRedis.get()'
        self._clean_it()
        data = 'foo'.encode()
        qr = QueueRedis(**redis_params)
        elem = qr.add(data)
        qr.lock(elem)
        self.assertEqual(qr.get(elem), data)
        qr.lock(elem)
        qr.remove(elem)
        self._clean_it()

    def test05count(self):
        'QueueRedis.count()'
        self._clean_it()
        qr = QueueRedis(**redis_params)
        # add "normal" element
        elem = qr.add('foo')
        self.assertEqual(qr.count(), 1)
        qr.lock(elem)
        qr.remove(elem)
        self._clean_it()

    def test06remove(self):
        'QueueRedis.remove()'
        self._clean_it()
        qr = QueueRedis(**redis_params)
        elems = []
        for _ in range(5):
            elems.append(qr.add('foo'))
        self.assertEqual(qr.count(), 5)
        for elem in qr:
            qr.lock(elem)
            qr.remove(elem)
        self.assertEqual(qr.count(), 0)
        self._clean_it()

    def test07purge(self):
        'QueueRedis.purge()'
        qr = QueueRedis(**redis_params)
        qr.add('foo')
        self.assertEqual(qr.count(), 1)
        elem = qr.first()
        qr.lock(elem)
        elem_path_lock = elem + LOCKED_SUFFIX
        self.assert_(qr.get(elem_path_lock) is not None)
        time.sleep(2)
        qr.purge(maxlock=1)
        self.assert_(qr.get(elem_path_lock) is None)
        self.assertEqual(qr.count(), 1)

    def test08purge_multElement(self):
        'QueueRedis.purge() multiple elements'
        qr = QueueRedis(**redis_params)

        qr.add('foo')
        assert qr.count() == 1
        time.sleep(2)
        qr.add('bar')
        assert qr.count() == 2
        qr.purge()
        assert qr.count() == 2

        elem = qr.first()
        qr.lock(elem)
        qr.remove(elem)
        assert qr.count() == 1
        qr.purge()

        time.sleep(2)
        qr.add('baz')
        assert qr.count() == 2
        for elem in qr:
            qr.lock(elem)
        elem1 = qr.first()
        lock_path1 = elem1 + LOCKED_SUFFIX
        self.assert_(qr.get(lock_path1) is not None)
        qr._redis.set(lock_path1, time.time() - 11)
        qr.purge(maxlock=10)
        self.assert_(qr.get(lock_path1) is None)

        elem2 = qr.next()
        lock_path2 = elem2 + LOCKED_SUFFIX
        self.assert_(qr.get(lock_path2) is not None)

        self.assertEqual(qr.count(), 2)
        for elem in qr:
            qr.lock(elem)
            qr.remove(elem)
        self.assertEqual(qr.count(), 0)


def main():
    testcases = [TestQueueRedis]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))

if __name__ == "__main__":
    main()
