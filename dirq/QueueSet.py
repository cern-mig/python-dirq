
from dirq.QueueBase import QueueBase
from Exceptions import QueueError

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
        self.qset = [] # set of queues
        self.elts = [] # local (queue, element) cache
        self._next_exception = False

        self._add(*queues)

    def __iter__(self):
        """Return iterator over element names on the set of queues.
        """
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
        for q in self.qset:
            q._reset()
        self.elts = []

    def first(self):
        """Return the first element in the queue set and cache information
        about the next ones.
        Raise:
            OSError - can't list directories
        """
        self._reset()
        return self.next()

    def next(self):
        """Return (queue, next element) tuple from the queue set, only using
        cached information.
        Raise:
            StopIteration - when used as Python iterator via
                            __iter__() method
            OSError       - can't list element directories
        """
        if not self.elts:
            for q in self.qset:
                self.elts.append((q, q.next()))
            if not self.elts:
                return (None, None)
        self.elts.sort(key=lambda x: x[1])
        for i,qe in enumerate(self.elts):
            self.elts[i] = (qe[0], qe[0].next())
            if qe[1]:
                return qe
        if self._next_exception:
            self._next_exception = False
            raise StopIteration
        else:
            return (None, None)

    def count(self):
        """Return the number of elements in the queue set, regardless of
        their state.

        Raise:
            OSError - can't list/stat element directories
        """
        c = 0
        for q in self.qset:
            c += q.count()
        return c

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
        for q in queues:
            if type(q) in [list, tuple] and not type_queue:
                for _q in q:
                    if isinstance(_q, QueueBase):
                        if _q.id in [x.id for x in self.qset]:
                            raise QueueError("queue already in the set: %s"%\
                                              _q.path)
                        self.qset.append(_q.copy())
                    else:
                        raise TypeError("QueueBase objects expected.")
                break
            elif isinstance(q, QueueBase):
                type_queue = True
                self.qset.append(q.copy())
            else:
                raise TypeError("expected QueueBase object(s) or list/tuple of "+\
                                 "QueueBase objects")

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

    def remove(self, queue):
        """Remove a queue and its respective elements from in memory cache.
        Arguments:
            queue - queue to be removed
        Raise:
            TypeError - wrong queue object type provided
        """
        if not isinstance(queue, QueueBase):
            raise TypeError("QueueBase objects expected.")
        for i,q in enumerate(self.qset):
            if queue.id == q.id:
                del self.qset[i]
                if self.elts:
                    del self.elts[i]

