# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile
import time
import unittest

from dirq import queue
from dirq.queue import Queue, QueueError

__all__ = ['TestQueue', 'TestQueueModuleFunctions']


class TestDirQueue(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='dirq')
        self.qdir = self.tempdir + '/dirq'

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)


class TestQueue(TestDirQueue):

    def test1init(self):
        """ Queue.__init__() """
        path = self.tempdir + '/aaa/bbb/ccc'
        umask = None
        maxelts = 10
        good_schemas = [
            {'a': 'binary'},
            {'a': 'binary*'},
            {'a': 'string'},
            {'a': 'string*'},
            {'a': 'table'},
            {'a': 'binary?', 'b': 'binary'},
            {'a': 'binary?', 'b': 'binary*'},
            {'a': 'string', 'b': 'binary?*'}, ]
        for schema in good_schemas:
            try:
                Queue(path, umask=umask, maxelts=maxelts, schema=schema)
            except Exception:
                error = sys.exc_info()[1]
                self.fail("Shouldn't have failed. Exception: %s" % error)
        bad_schemas = [['foo'],
                       {'foo': 1},
                       {'a': 'binary?'}]
        for schema in bad_schemas:
            self.assertRaises(
                QueueError, Queue, path,
                umask=umask, maxelts=maxelts, schema=schema)
        bad_schemas = [{'a': 'strings'}, {'a': 'table??'}]
        for schema in bad_schemas:
            self.assertRaises(
                QueueError, Queue, path,
                umask=umask, maxelts=maxelts, schema=schema)

    def test2copy(self):
        """ Queue.copy(). """
        q = Queue(self.qdir, schema={'a': 'string'})
        q1 = q.copy()
        q.foo = 1
        try:
            q1.foo
        except AttributeError:
            pass
        else:
            self.fail('Not a copy, but reference.')

    def test3_is_locked(self):
        """ Queue._is_locked_*() """
        q = Queue(self.qdir, schema={'a': 'string'})

        assert q._is_locked_nlink('') is False
        assert q._is_locked_nlink('not_there') is False
        os.mkdir(self.qdir + '/a')
        assert q._is_locked_nlink('a') is False
        os.mkdir(self.qdir + '/a/%s' % queue.LOCKED_DIRECTORY)
        assert q._is_locked_nlink('a') is True
        time.sleep(1)
        assert q._is_locked_nlink('a', time.time()) is True

        assert q._is_locked_nonlink('') is False
        assert q._is_locked_nonlink('not_there') is False
        os.mkdir(self.qdir + '/b')
        assert q._is_locked_nonlink('b') is False
        os.mkdir(self.qdir + '/b/%s' % queue.LOCKED_DIRECTORY)
        assert q._is_locked_nonlink('b') is True
        time.sleep(1)
        assert q._is_locked_nonlink('b', time.time()) is True

    def test4_insertion_directory(self):
        """ Queue._insertion_directory() """
        q = queue.Queue(self.qdir, schema={'a': 'string'})
        q.maxelts = 1
        name0 = '%08x' % 0
        assert q._insertion_directory() == name0
        assert os.path.exists(self.qdir + '/' + name0)
        os.mkdir('%s/%s/%s' % (self.qdir, name0, queue._name(q.rndhex)))
        name1 = '%08x' % 1
        assert q._insertion_directory() == name1
        assert os.path.exists(self.qdir + '/' + name1)

    def test5add(self):
        """ Queue.add() """
        q = queue.Queue(self.qdir, schema={'a': 'string'})
        q.add({'a': 'a\n'})

        assert os.listdir(self.qdir + '/' + queue.TEMPORARY_DIRECTORY) == []
        data_file = '%s/%s/%s/a' % (self.qdir, '%08x' % 0,
                                    os.listdir('%s/%08x' % (self.qdir, 0))[-1])
        assert os.path.exists(data_file)
        assert open(data_file).read() == 'a\n'

    def test6touch(self):
        """ Queue.touch() """
        q = queue.Queue(self.qdir, schema={'a': 'string'})
        q.add({'a': 'a\n'})
        e = q.first()
        element_dir = q.path + '/' + e
        old_time = time.time() - 10
        os.utime(element_dir, (old_time, old_time))
        mtime = os.stat(element_dir).st_mtime
        q.touch(e)
        assert os.stat(element_dir).st_mtime > mtime


class TestQueueModuleFunctions(TestDirQueue):

    def test2_check_element(self):
        """ queue._check_element() """
        queue._check_element('0' * 8 + '/' + '0' * 14)
        queue._check_element('f' * 8 + '/' + 'f' * 14)
        for e in ['f' * 7 + '/' + 'f' * 14,
                  'f' * 9 + '/' + 'f' * 14,
                  'f' * 8 + '/' + 'f' * 13,
                  'f' * 8 + '/' + 'f' * 15,
                  'f' * 8 + '/' + 'g' * 14,
                  'g' * 8 + '/' + 'f' * 14,
                  ]:
            self.assertRaises(queue.QueueError, queue._check_element, (e))

    def test3_hash2string(self):
        """ queue._hash2string() """
        assert queue._hash2string({'a1': 'a2'}) == 'a1\ta2\n'
        assert queue._hash2string({'a1\\': 'a2'}) == 'a1\\\\\ta2\n'
        assert queue._hash2string({'a1	a2': 'a3	a4'}) == 'a1\\ta2\ta3\\ta4\n'
        assert queue._hash2string({'a1	\na2': 'a3	\na4'}) == \
            'a1\\t\\na2\ta3\\t\\na4\n'

    def test3_string2hash(self):
        """ queue._string2hash() """
        assert queue._string2hash('a1\ta2\nb1\tb2') == {'a1': 'a2', 'b1': 'b2'}
        assert queue._string2hash('a1\x5c\ta2\nb1\tb2\\') == \
            {'a1\x5c': 'a2', 'b1': 'b2\\'}
        for value in ['a', ]:
            self.assertRaises(queue.QueueError, queue._string2hash, (value))

    def test3_hash2string2hash(self):
        """ queue._hash2string()+queue._string2hash() """
        example = {"hi\\t\th\nere": "h\\ello\twor\nld"}
        converted = queue._hash2string(example)
        assert converted == "hi\\\\t\\th\\nere\th\\\\ello\\twor\\nld\n"
        back = queue._string2hash(converted)
        assert back == example


def main():
    """ Main. """
    testcases = [TestQueue,
                 TestQueueModuleFunctions]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))


if __name__ == "__main__":
    main()
