"""Interface to elements on a set of directory based queues.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""

import dirq

from dirq.QueueBase import QueueBase
from dirq.Exceptions import QueueError

__version__ = dirq.VERSION
__author__ = dirq.AUTHOR
__date__ = dirq.DATE


class QueueSet(object):
    """Interface to elements on a set of directory based queues.
    """
    def __init__(self, *queues):
        """Generate queue set on the given lists of queues. Copies of the
        object instances are used.

        Arguments:
            *queues - QueueSet([q1,..]/(q1,..)) or QueueSet(q1,..)

        Raise:
            QueueError - queues should be list/tuple or Queue object
            TypeError  - one of objects provided is not instance of Queue
        """
        self.qset = []  # set of queues
        self.elts = []  # local (queue, element) cache
        self._next_exception = False

        self._add(*queues)

    def __iter__(self):
        """ Return iterator over element names on the set of queues. """
        self._reset()
        self._next_exception = True
        return self

    def names(self):
        """Return iterator over element names on the set of queues.
        """
        return self.__iter__()

    def _reset(self):
        """Regenerate lists of intermediate directories and drop cached
        elements lists.

        Raise:
            OSError - can't list directories
        """
        for queue in self.qset:
            queue._reset()
        self.elts = []

    def first(self):
        """Return the first element in the queue set and cache information
        about the next ones.

        Raise:
            OSError - can't list directories
        """
        self._reset()
        return self.next()

    def __next__(self):
        """Return (queue, next element) tuple from the queue set, only using
        cached information.

        Raise:
            StopIteration - when used as Python iterator via
                            __iter__() method

            OSError       - can't list element directories
        """
        if not self.elts:
            for queue in self.qset:
                self.elts.append((queue, queue.next()))
            if not self.elts:
                return (None, None)
        self.elts.sort(key=lambda x: x[1])
        for index, queue_elt in enumerate(self.elts):
            self.elts[index] = (queue_elt[0], queue_elt[0].next())
            if queue_elt[1]:
                return queue_elt
        if self._next_exception:
            self._next_exception = False
            raise StopIteration
        else:
            return (None, None)
    next = __next__

    def count(self):
        """Return the number of elements in the queue set, regardless of
        their state.

        Raise:
            OSError - can't list/stat element directories
        """
        count = 0
        for queue in self.qset:
            count += queue.count()
        return count

    def _add(self, *queues):
        """Add lists of queues to existing ones. Copies of the object
        instances are used.

        Arguments:
            *queues - add([q1,..]/(q1,..)) or add(q1,..)

        Raise:
            QueueError - queue already in the set
            TypeError  - wrong queue object type provided
        """
        type_queue = False
        for queue in queues:
            if type(queue) in [list, tuple] and not type_queue:
                for _queue in queue:
                    if isinstance(_queue, QueueBase):
                        if _queue.id in [x.id for x in self.qset]:
                            raise QueueError("queue already in the set: %s" %
                                             _queue.path)
                        self.qset.append(_queue.copy())
                    else:
                        raise TypeError("QueueBase objects expected.")
                break
            elif isinstance(queue, QueueBase):
                type_queue = True
                self.qset.append(queue.copy())
            else:
                raise TypeError("expected QueueBase object(s) or list/tuple "
                                "of QueueBase objects")

    def add(self, *queues):
        """Add lists of queues to existing ones. Copies of the object
        instances are used.

        Arguments:
            *queues - add([q1,..]/(q1,..)) or add(q1,..)

        Raise:
            QueueError - queue already in the set
            TypeError  - wrong queue object type provided
        """
        self._add(*queues)
        self._reset()

    def remove(self, given_queue):
        """Remove a queue and its respective elements from in memory cache.

        Arguments:
            queue - queue to be removed

        Raise:
            TypeError - wrong queue object type provided
        """
        if not isinstance(given_queue, QueueBase):
            raise TypeError("QueueBase objects expected.")
        for index, queue in enumerate(self.qset):
            if given_queue.id == queue.id:
                del self.qset[index]
                if self.elts:
                    del self.elts[index]
