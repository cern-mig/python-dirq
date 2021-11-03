"""
QueueNull - object oriented interface to a null directory based queue.

A port of Perl module Directory::Queue::Null
http://search.cpan.org/dist/Directory-Queue/
The documentation from Directory::Queue::Null module was
adapted for Python.

=================
QueueNull class
=================

:py:class:`QueueNull` - null directory based queue.

Usage::

    from dirq.QueueNull import QueueNull

    # sample producer

    dirq = QueueNull()
    for count in range(1,101):
        name = dirq.add("element %i\\n" % count)


Description
-----------

    The goal of this module is to offer a "null" queue system using the
    same API as the other directory queue implementations. The queue will
    behave like a black hole: added data will disappear immediately so the
    queue will therefore always appear empty.

    This can be used for testing purposes or to discard data like one
    would do on Unix by redirecting output to */dev/null*.

    Please refer to :py:mod:`dirq.queue` for general information about
    directory queues.


Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""

import os
from dirq.QueueBase import QueueBase


class QueueNull(QueueBase):
    """
    QueueNull
    """
    def __init__(self):
        """ Constructor, this does nothing."""
        pass

    def add(self, data):
        """Add data to the queue, this does nothing. """
        return ""

    add_ref = add  # to comply with the Perl Directory::Queue interface

    def add_path(self, path):
        """Add the given file (identified by its path) to the queue,
        this will therefore *remove the file.*
        """
        os.unlink(path)
        return ""

    def get(self, name):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: get()")

    get_ref = get  # to comply with the Perl Directory::Queue interface

    def get_path(self, name):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: get_path()")

    def lock(self, name, permissive=True):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: lock()")

    def unlock(self, name, permissive=False):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: unlock()")

    def remove(self, name):
        """ Not supported method. """
        raise NotImplementedError("unsupported method: remove()")

    def count(self):
        """Return the number of elements in the queue, which means
        it always return 0.
        """
        return 0

    def purge(self, maxtemp=300, maxlock=600):
        """ Purge the queue, this does nothing. """
        pass
