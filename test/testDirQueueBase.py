# -*- coding: utf-8 -*-

import codecs
import errno
import os
import shutil
import sys
import tempfile
import unittest

from dirq import QueueBase
from dirq.QueueBase import QueueBase as QueueBaseClass

__all__ = ['TestQueueBase', 'TestQueueBaseModuleFunctions']


class TestDirQueue(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='dirq-base')
        self.qdir = self.tempdir + '/dirq'
        self.tempfile = self.tempdir + '/file'

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)


class TestQueueBase(TestDirQueue):

    def test1init(self):
        'QueueBase.__init__()'
        path = self.tempdir + '/aaa/bbb/ccc'
        QueueBaseClass(path, umask=None)
        assert os.path.exists(path) is True


class TestQueueBaseModuleFunctions(TestDirQueue):

    def test1_special_mkdir(self):
        'QueueBase._special_mkdir()'
        assert QueueBase._special_mkdir(self.qdir) is True
        assert QueueBase._special_mkdir(self.qdir) is False
        # test against a file
        test_file = os.path.join(self.qdir, 'foo')
        open(test_file, 'w').write('bar')
        self.assertRaises(OSError, QueueBase._special_mkdir, (test_file))

    def test2_name(self):
        'QueueBase._name()'
        n = QueueBase._name(7)
        assert len(n) == 14
        assert n.endswith('%01x' % 7)

    def test3_file_create(self):
        'QueueBase._file_create()'
        # File in non existent directory should produce ENOENT.
        fn = os.getcwd() + "/nodir/nofile"
        try:
            QueueBase._file_create(fn, 0, False)
        except OSError:
            error = sys.exc_info()[1]
            assert error.errno == errno.ENOENT

        QueueBase._file_create(self.tempfile, 0, False)
        self.assertRaises(OSError,
                          QueueBase._file_create,
                          *(self.tempfile, 0, False))
        os.unlink(self.tempfile)
        # utf8 data
        QueueBase._file_create(self.tempfile, 0, True)
        self.assertRaises(OSError,
                          QueueBase._file_create,
                          *(self.tempfile, 0, True))

    def test4_file_write(self):
        'QueueBase._file_write()'
        QueueBase._file_write(self.tempfile, 0, False, 'a\n'.encode())
        os.unlink(self.tempfile)
        QueueBase._file_write(self.tempfile, 0,
                              False, ('a' * (2 ** 10) * 10).encode())
        os.unlink(self.tempfile)
        for t in [1, [], (), {}, object]:
            self.assertRaises(TypeError, QueueBase._file_write, ('', t))

    def test5_file_read(self):
        'QueueBase._file_read()'
        text = 'hello\n'.encode()
        open(self.tempfile, 'wb').write(text)
        text_in = QueueBase._file_read(self.tempfile, False)
        self.assertEqual(text, text_in)
        # utf8
        try:
            text = 'Élément \u263A\n'.decode("utf-8")
        except AttributeError:
            text = 'Élément \u263A\n'
        codecs.open(self.tempfile, 'w', 'utf8').write(text)
        text_in = QueueBase._file_read(self.tempfile, True)
        self.assertEqual(text, text_in)


def main():
    testcases = [TestQueueBase,
                 TestQueueBaseModuleFunctions]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))


if __name__ == "__main__":
    main()
