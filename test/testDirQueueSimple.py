# -*- coding: utf-8 -*-

import os
import shutil
import unittest
import time

from dirq.QueueSimple import QueueSimple, LOCKED_SUFFIX
import tempfile

__all__ = ['TestQueueSimple']


class TestDirQueue(unittest.TestCase):

    def setUp(self):
        self.qdir = tempfile.mkdtemp(prefix='directory-qsimple')
        shutil.rmtree(self.qdir, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(self.qdir, ignore_errors=True)


class TestQueueSimple(TestDirQueue):

    def test01init(self):
        'QueueSimple.__init__()'
        path = self.qdir + '/aaa/bbb/ccc'
        granularity = 30
        qs = QueueSimple(path, granularity=granularity)
        assert qs.path == path
        assert qs.granularity == granularity

    def test02_add_data(self):
        'QueueSimple._add_data()'
        data = 'foo'
        qs = QueueSimple(self.qdir)
        _subdirName, _fullFnOrig = qs._add_data(data)
        subdirs = os.listdir(self.qdir)
        assert len(subdirs) == 1
        assert _subdirName == subdirs[0]
        subdir = self.qdir + '/' + subdirs[0]
        files = os.listdir(subdir)
        assert len(files) == 1
        fn = subdir + '/' + files[0]
        assert _fullFnOrig == fn
        assert open(fn, 'r').read() == data

    def test03_add_path(self):
        'QueueSimple._add_path()'
        data = 'abc'
        qs = QueueSimple(self.qdir)
        _dir = 'elems'
        elems_dir = self.qdir + '/' + _dir
        os.mkdir(elems_dir)
        _tmpName = elems_dir + '/elem.tmp'
        open(_tmpName, 'w').write(data)
        newName = qs._add_path(_tmpName, _dir)
        assert len(os.listdir(elems_dir)) == 1
        assert open(self.qdir + '/' + newName).read() == data

    def test04add(self):
        'QueueSimple.add()'
        data = 'foo bar'
        qs = QueueSimple(self.qdir)
        elem = qs.add(data)
        assert open(self.qdir + '/' + elem).read() == data

    def test05add_path(self):
        'QueueSimple.add_path()'
        qs = QueueSimple(self.qdir)
        data = 'foo'
        path = self.qdir + '/foo.bar'
        fh = open(path, 'w')
        fh.write(data)
        fh.flush()
        fh.close()
        elem = qs.add_path(path)
        assert open(self.qdir + '/' + elem).read() == data
        self.failIf(os.path.exists(path))

    def test06lockunlok(self):
        'QueueSimple.lock()'
        qs = QueueSimple(self.qdir)
        data = 'foo'
        elem_name = 'foo.bar'
        elem_full_path = self.qdir + '/' + elem_name
        fh = open(elem_full_path, 'w')
        fh.write(data)
        fh.flush()
        fh.close()
        self.assertEqual(qs.lock(elem_name), 1)
        self.failUnless(os.path.exists(elem_full_path + LOCKED_SUFFIX))
        qs.unlock(elem_name)

    def test07get(self):
        'QueueSimple.get()'
        data = 'foo'.encode()
        qs = QueueSimple(self.qdir)
        elem = qs.add(data)
        qs.lock(elem)
        self.assertEqual(qs.get(elem), data)

    def test08count(self):
        'QueueSimple.count()'
        qs = QueueSimple(self.qdir)
        # add "normal" element
        qs.add('foo')
        # simply add a file (fake element) into the elements directory
        fake_elem = os.listdir(self.qdir)[0] + '/' + 'foo.bar'
        open(self.qdir + '/' + fake_elem, 'w').write('')
        self.assertEqual(qs.count(), 1)

    def test09remove(self):
        'QueueSimple.remove()'
        qs = QueueSimple(self.qdir, granularity=1)
        for _ in range(5):
            qs.add('foo')
        assert qs.count() == 5
        for elem in qs:
            qs.lock(elem)
            qs.remove(elem)
        self.assertEqual(qs.count(), 0)

    def test10purge_oneDirOneElement(self):
        'QueueSimple.purge() one directory & element'
        qs = QueueSimple(self.qdir)
        qs.add('foo')
        self.assertEqual(qs.count(), 1)
        elem = qs.first()
        qs.lock(elem)
        elem_path_lock = self.qdir + '/' + elem + LOCKED_SUFFIX
        self.assert_(os.path.exists(elem_path_lock) is True)
        time.sleep(2)
        qs.purge(maxlock=1)
        self.assert_(os.path.exists(elem_path_lock) is False)
        self.assertEqual(qs.count(), 1)
        self.assertEqual(len(os.listdir(self.qdir)), 1)

    def test11purge_multDirMultElement(self):
        'QueueSimple.purge() multiple directories & elements'
        qs = QueueSimple(self.qdir, granularity=1)

        qs.add('foo')
        assert qs.count() == 1
        time.sleep(2)
        qs.add('bar')
        assert qs.count() == 2
        assert len(os.listdir(self.qdir)) == 2
        qs.purge()
        assert qs.count() == 2

        elem = qs.first()
        qs.lock(elem)
        qs.remove(elem)
        assert qs.count() == 1
        qs.purge()
        assert len(os.listdir(self.qdir)) == 1

        time.sleep(2)
        qs.add('baz')
        assert len(os.listdir(self.qdir)) == 2
        for elem in qs:
            qs.lock(elem)
        elem1 = qs.first()
        lock_path1 = self.qdir + '/' + elem1 + LOCKED_SUFFIX
        assert os.path.exists(lock_path1) is True
        os.utime(lock_path1, (time.time() - 25, time.time() - 25))
        qs.purge(maxlock=10)
        assert os.path.exists(lock_path1) is False

        elem2 = qs.next()
        lock_path2 = self.qdir + '/' + elem2 + LOCKED_SUFFIX
        assert os.path.exists(lock_path2) is True


def main():
    testcases = [TestQueueSimple]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))

if __name__ == "__main__":
    main()
