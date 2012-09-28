"""
QueueRedis - object oriented interface to a redis based queue.

=================
QueueRedis class
=================

:py:class:`QueueRedis` - redis based queue.

Usage::

    from dirq.QueueRedis import QueueRedis

    # sample producer

    redisq = QueueRedis()
    for count in range(1,101):
        name = redisq.add("element %i\\n" % count)
        print("# added element %i as %s" % (count, name))

    # sample consumer

    redisq = QueueRedis('/tmp/test')
    for name in redisq:
        if not redisq.lock(name):
            continue
        print("# reading element %s" % name)
        data = redisq.get(name)
        # one could use redisq.unlock(name) to only browse the queue...
        redisq.remove(name)

Description
-----------

    The goal of this module is to offer a "redis" queue system using the
    same API as the other directory queue implementations.

    Please refer to :py:mod:`dirq.queue` for general information about
    directory queues.


Author
------

Massimo Paladin \<massimo.paladin@gmail.com\>

License and Copyright
---------------------

ASL 2.0

Copyright (C) 2010-2012
"""

import re
import sys
import time
from dirq.QueueBase import QueueBase, _name
from dirq.Exceptions import QueueError
import redis
from redis.exceptions import ConnectionError

# suffix indicating a locked element
LOCKED_SUFFIX = ".lck"
ISLOCK = re.compile(".*\.lck$")


class QueueRedis(QueueBase):
    """
    QueueRedis
    """
    def __init__(self, path, host='localhost',
                 port=6379, password=None, maxlock=600):
        """ Constructor:
        * path - prefix for the key to be used in redis
        * host - redis server's host
        * port - redis server's port
        * password - redis server's password
        * maxlock - life of the lock
        """
        super(QueueRedis, self).__init__(path)

        self._path = path
        self._host = host
        self._port = port
        self._password = password
        self._redis = redis.Redis(host=host, port=port, password=password)
        self.maxlock = maxlock

    def add(self, data):
        """Add data to the queue.

        Return: element name.
        """
        name = "%s.%s" % (self._path, _name())
        try:
            self._redis.set(name, data)
        except ConnectionError:
            error = sys.exc_info()[1]
            raise QueueError("Redis connection error %s:%s: %s" %
                             (self._host, self._port, error))
        return name

    add_ref = add
    "Defined to comply with Directory::Queue interface."

    def add_path(self, path):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: get_path()")

    def get(self, name):
        """ Get the element identified by its name. """
        try:
            element = self._redis.get(name)
        except ConnectionError:
            raise QueueError("Redis connection error: %s:%s" %
                             (self._host, self._port))
        if element is not None:
            return element.decode()
        return element

    get_ref = get
    "Get locked element. Defined to comply with Directory::Queue interface."

    def get_path(self, name):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: get_path()")

    def lock(self, name):
        """
        Lock an element.

        For locking following recommendations at:
        http://redis.io/commands/setnx

        Arguments:
            name - name of an element

        Return:

        * true on success
        * false in case the element could not be locked
        """
        elem_key = name
        lock_key = "%s%s" % (name, LOCKED_SUFFIX)
        value = self._redis.get(elem_key)
        if value is None:
            raise QueueError("element not found: %s" % name)
        if self._redis.setnx(lock_key, time.time()):
            # lock gained
            return True
        current = self._redis.get(lock_key)
        now = time.time()
        # expired lock? get it!
        if current and (float(current) + self.maxlock) < now and \
                self._redis.getset(lock_key, now) == now:
            return True
        return False

    def unlock(self, name, permissive=False):
        """
        Unlock an element.

        For unlocking following recommendations at:
        http://redis.io/commands/setnx

        Arguments:
            name - name of an element

        Return:

        * true on success
        * false in case the element could not be unlocked
        """
        try:
            result = self._redis.delete("%s%s" %
                                        (name, LOCKED_SUFFIX))
        except ConnectionError:
            raise QueueError("Redis connection error: %s:%s" %
                             (self._host, self._port))
        return result

    def remove(self, name):
        """ Remove a locked element from the queue. """
        try:
            if not self._redis.delete("%s%s" % (name, LOCKED_SUFFIX)):
                raise QueueError("not found: %s%s" % (name, LOCKED_SUFFIX))
            if not self._redis.delete(name):
                raise QueueError("not found: %s" % (name, ))
        except ConnectionError:
            raise QueueError("Redis connection error: %s:%s" %
                             (self._host, self._port))

    def count(self):
        """ Return the number of elements in the queue. """
        elts = [el for el in self._redis.keys("%s.*" % self._path)
                if not ISLOCK.match(el.decode())]
        return len(elts)

    def purge(self, maxlock=600):
        """ Purge the queue by removing expired locks.

        maxlock - maximum time for a locked element (in seconds, default 600);
                  if set to 0, locked elements will not be unlocked"""
        if maxlock == 0:
            return
        elems = self._redis.keys("%s.*%s" % (self._path, LOCKED_SUFFIX))
        for elem in elems:
            value = self._redis.get(elem)
            now = time.time()
            if value and (float(value) + maxlock) < now:
                self._redis.delete(elem)

    def _reset(self):
        """ Regenerate list, drop cached elements list. """
        self.elts = []
        tmp = [el.decode() for el in self._redis.keys("%s.*" % self._path)
               if ISLOCK.match(el.decode()) is None]
        if tmp:
            tmp.sort()
            self.elts = tmp

    def __next__(self):
        """Return name of the next element in the queue, only using cached
        information. When queue is empty, depending on the iterator
        protocol - return empty string or raise StopIteration.

        Return:
            name of the next element in the queue

        Raise:
            StopIteration - when used as Python iterator via
                            __iter__() method
        """
        if self.elts:
            return self.elts.pop(0)
        if self._next_exception:
            self._next_exception = False
            raise StopIteration
        else:
            return ''
    next = __next__
