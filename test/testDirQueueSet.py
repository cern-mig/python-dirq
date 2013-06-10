# -*- coding: utf-8 -*-

import os
import shutil
import tempfile
import unittest

__all__ = ['TestQueueSet']

from dirq.queue import Queue
from dirq.Exceptions import QueueError
from dirq.QueueSet import QueueSet


class TestDirQueue(unittest.TestCase):

    def setUp(self):
        self.path = tempfile.mkdtemp()
        for i in range(1, 5):
            setattr(self, 'p%i' % i, '%s/%s' % (self.path, 'p%i' % i))
        shutil.rmtree(self.path, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)


class TestQueueSet(TestDirQueue):
    """ TestQueueSet """

    def test1_init(self):
        """ QueueSet.__init__() """
        self.failUnlessRaises(TypeError, QueueSet, ('a'))
        self.failUnlessRaises(TypeError, QueueSet,
                              (Queue(self.p1, schema={'data': 'string'}), 'a'))
        self.failUnlessRaises(TypeError, QueueSet,
                              ([Queue(self.p1,
                                      schema={'data': 'string'}), 'a']))
        self.failUnlessRaises(TypeError, QueueSet, ([1, 2]))
        self.failUnlessRaises(TypeError, QueueSet, ((1, 2)))

    def test2_addremove(self):
        """ QueueSet.add()/remove() """
        q1 = Queue(self.p1, schema={'data': 'string'})
        q2 = Queue(self.p2, schema={'data': 'string'})
        q3 = Queue(self.p3, schema={'data': 'string'})
        q4 = Queue(self.p4, schema={'data': 'string'})
        for i in range(10):
            q1.add({'data': '%i A\n' % i})
            q2.add({'data': '%i A\n' % i})
            q3.add({'data': '%i A\n' % i})
            q4.add({'data': '%i A\n' % i})
        qs = QueueSet([q1, q2])
        qs.add(q3)
        qs.remove(q1)
        qs.add(q1, q4)
        self.failUnlessRaises(QueueError, qs.add, ([q1]))

    def test3_firstnext(self):
        """ QueueSet.first()/next() """
        q1 = Queue(self.p1, schema={'data': 'string'})
        q2 = Queue(self.p2, schema={'data': 'string'})
        for i in range(10):
            q1.add({'data': '%i A\n' % i})
            q2.add({'data': '%i A\n' % i})
        qs = QueueSet([q1, q2])
        e = qs.first()
        assert isinstance(e, tuple)
        assert isinstance(e[0], Queue)
        assert isinstance(e[1], str)
        while e[0]:
            e = qs.next()


def main():
    testcases = [TestQueueSet]
    for tc in testcases:
        unittest.TextTestRunner(verbosity=2).\
            run(unittest.TestLoader().loadTestsFromTestCase(tc))

if __name__ == "__main__":
    main()
