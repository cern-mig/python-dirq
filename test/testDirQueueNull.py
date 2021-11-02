# -*- coding: utf-8 -*-

import os
import shutil
import unittest

from dirq.QueueNull import QueueNull
import tempfile

__all__ = ['TestQueueNull']


def assert_fail(exc, callable, *args, **kwargs):
    try:
        callable(*args, **kwargs)
        raise AssertionError("An exception was expected")
    except exc:
        # fine
        pass


class TestDirQueue(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='dirq-null')
        self.qdir = self.tempdir + '/dirq'

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)


class TestQueueNull(TestDirQueue):

    def test01init(self):
        'QueueNull.__init__()'
        qn = QueueNull()

    def test02add(self):
        'QueueNull.add()'
        data = 'foo bar'
        qn = QueueNull()
        elem = qn.add(data)
        assert elem == ""

    def test03add_path(self):
        'QueueNull.add_path()'
        qn = QueueNull()
        data = 'foo'
        path = self.tempdir + '/foo.bar'
        fh = open(path, 'w')
        fh.write(data)
        fh.flush()
        fh.close()
        elem = qn.add_path(path)
        self.assertFalse(os.path.exists(path))

    def test04lockunlok(self):
        'QueueNull.lock()'
        qn = QueueNull()
        assert_fail(NotImplementedError, qn.lock, "")
        assert_fail(NotImplementedError, qn.unlock, "")

    def test05get(self):
        'QueueNull.get()'
        qn = QueueNull()
        assert_fail(NotImplementedError, qn.get, "")

    def test06count(self):
        'QueueNull.count()'
        qn = QueueNull()
        qn.add('foo1')
        assert qn.count() == 0
        qn.add('foo2')
        assert qn.count() == 0

    def test07remove(self):
        'QueueNull.remove()'
        qn = QueueNull()
        assert_fail(NotImplementedError, qn.remove, "")

    def test08purge_oneDirOneElement(self):
        'QueueNull.purge() one directory & element'
        qn = QueueNull()
        qn.purge()


def main():
    testcases = [TestQueueNull]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))


if __name__ == "__main__":
    main()
