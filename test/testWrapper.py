# -*- coding: utf-8 -*-

import inspect
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

__all__ = ['TestQueueWrapper']


class TestDirQueue(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='dirq-wrapper')
        self.qdir = self.tempdir + '/dirq'
        me = inspect.getfile(inspect.currentframe())
        self.tdir = os.path.dirname(os.path.abspath(me))
        self.args = ["-p", self.qdir]
        if __name__ == "__main__":
            self.args.append("-d")

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)


class TestQueueWrapper(TestDirQueue):

    def test01(self):
        'dqt.py normal'
        tcmd = [sys.executable, self.tdir + "/dqt.py"] + self.args
        rc = subprocess.call(tcmd +
                             ["-c", "1000", "--type", "normal", "simple"])
        assert rc == 0

    def test02(self):
        'dqt.py simple'
        tcmd = [sys.executable, self.tdir + "/dqt.py"] + self.args
        rc = subprocess.call(tcmd +
                             ["-c", "1000", "--type", "simple", "simple"])
        assert rc == 0

    def test03(self):
        'dqst.py complex'
        tcmd = [sys.executable, self.tdir + "/dqst.py"] + self.args
        rc = subprocess.call(tcmd +
                             ["-c", "200", "--type", "normal", "complex"])
        assert rc == 0

    def test04(self):
        'dqst.py iterate'
        tcmd = [sys.executable, self.tdir + "/dqt.py",
                "-c", "1000", "--type", "simple"]
        if __name__ == "__main__":
            tcmd.append("-d")
        rc = subprocess.call(tcmd + ["-p", self.tempdir + "/q1", "add"])
        assert rc == 0
        rc = subprocess.call(tcmd + ["-p", self.tempdir + "/q2", "add"])
        assert rc == 0
        tcmd = [sys.executable, self.tdir + "/dqst.py", "--type", "simple"]
        if __name__ == "__main__":
            tcmd.append("-d")
        paths = "%s/q1,%s/q2" % (self.tempdir, self.tempdir)
        rc = subprocess.call(tcmd + ["-p", paths, "iterate"])
        assert rc == 0


def main():
    testcases = [TestQueueWrapper]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))


if __name__ == "__main__":
    main()
