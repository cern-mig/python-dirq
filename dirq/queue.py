"""Directory based queue.

A port of Perl module Directory::Queue
http://search.cpan.org/dist/Directory-Queue/
The documentation from Directory::Queue module was adapted for Python.

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue.

Provides
========

Classes:

* :py:class:`dirq.queue.Queue`       directory based queue
* :py:class:`dirq.QueueSimple.QueueSimple` simple directory based queue
* :py:class:`dirq.queue.QueueSet`    set of directory based queues
* :py:class:`dirq.Exceptions.QueueError`  exception

Documentation
=============

===========
Queue class
===========

:py:class:`dirq.queue.Queue` - directory based queue.

Usage::

    from dirq.queue import Queue

    # simple schema:
    #  - there must be a "body" which is a string
    #  - there can be a "header" which is a table/dictionary

    schema = {"body": "string", "header": "table?"}
    queuedir = "/tmp/test"

    # sample producer

    dirq = Queue(queuedir, schema=schema)
    import os
    for count in range(1,101):
        name = dirq.add({"body"  : "element %i"%count,
                         "header": dict(os.environ)})
        print("# added element %i as %s" % (count, name))

    # sample consumer

    dirq = Queue(queuedir, schema=schema)
    name = dirq.first()
    while name:
        if not dirq.lock(name):
            name = dirq.next()
            continue
        print("# reading element %s" % name)
        data = dirq.get(name)
        # one can use data['body'] and data['header'] here...
        # one could use dirq.unlock(name) to only browse the queue...
        dirq.remove(name)
        name = dirq.next()

Terminology
-----------

    An element is something that contains one or more pieces of data. A
    simple string may be an element but more complex schemas can also be
    used, see the *Schema* section for more information.

    A queue is a "best effort FIFO" collection of elements.

    It is very hard to guarantee pure FIFO behavior with multiple writers
    using the same queue. Consider for instance:

    . Writer1: calls the add() method
    . Writer2: calls the add() method
    . Writer2: the add() method returns
    . Writer1: the add() method returns

    Who should be first in the queue, Writer1 or Writer2?

    For simplicity, this implementation provides only "best effort FIFO",
    i.e. there is a very high probability that elements are processed in
    FIFO order but this is not guaranteed. This is achieved by using a
    high-resolution time function and having elements sorted by the time
    the element's final directory gets created.

Locking
-------

    Adding an element is not a problem because the add() method is atomic.

    In order to support multiple processes interacting with the same queue,
    advisory locking is used. Processes should first lock an element before
    working with it. In fact, the get() and remove() methods raise an
    exception if they are called on unlocked elements.

    If the process that created the lock dies without unlocking the ele-
    ment, we end up with a staled lock. The purge() method can be used to
    remove these staled locks.

    An element can basically be in only one of two states: locked or
    unlocked.

    A newly created element is unlocked as a writer usually does not need
    to do anything more with the element once dropped in the queue.

    Iterators return all the elements, regardless of their states.

    There is no method to get an element state as this information is usu-
    ally useless since it may change at any time. Instead, programs should
    directly try to lock elements to make sure they are indeed locked.

Constructor
-----------

    For the signature of the Queue constructor see documentation to the
    respective __init__() method.

Schema
------
    The schema defines how user supplied data is stored in the queue. It is
    only required by the add() and get() methods.

    The schema must be a dictionary containing key/value pairs.

    The key must contain only alphanumerical characters. It identifies the
    piece of data and will be used as file name when storing the data
    inside the element directory.

    The value represents the type of the given piece of data. It can be:

    binary
        the data is a sequence of binary bytes, it will be stored directly
        in a plain file with no further encoding

    string
        the data is a text string (i.e. a sequence of characters), it will
        be UTF-8 encoded

    table
        the data is a reference to a hash of text strings, it will be seri-
        alized and UTF-8 encoded before being stored in a file

    By default, all pieces of data are mandatory. If you append a question
    mark to the type, this piece of data will be marked as optional. See
    the comments in the *Usage* section for more information.

    To comply with Directory::Queue implementation it is allowed to
    append '*' (asterisk) to data type specification, which in
    Directory::Queue means switching to working with element references in
    add() and get() operations. This is irrelevant for the Python
    implementation.

Directory Structure
-------------------

    All the directories holding the elements and all the files holding the
    data pieces are located under the queue toplevel directory. This direc-
    tory can contain:

    temporary
        the directory holding temporary elements, i.e. the elements being
        added

    obsolete
        the directory holding obsolete elements, i.e. the elements being
        removed

    NNNNNNNN
        an intermediate directory holding elements; NNNNNNNN is an 8-digits
        long hexadecimal number

    In any of the above directories, an element is stored as a single
    directory with a 14-digits long hexadecimal name SSSSSSSSMMMMMR where:

    SSSSSSSS
        represents the number of seconds since the Epoch

    MMMMM
        represents the microsecond part of the time since the Epoch

    R   is a random digit used to reduce name collisions

    Finally, inside an element directory, the different pieces of data are
    stored into different files, named according to the schema. A locked
    element contains in addition a directory named "locked".

Security
--------

    There are no specific security mechanisms in this module.

    The elements are stored as plain files and directories. The filesystem
    security features (owner, group, permissions, ACLs...) should be used
    to adequately protect the data.

    By default, the process' umask is respected. See the class constructor
    documentation if you want an other behavior.

    If multiple readers and writers with different uids are expected, the
    easiest solution is to have all the files and directories inside the
    toplevel directory world-writable (i.e. umask=0). Then, the permissions
    of the toplevel directory itself (e.g. group-writable) are enough to
    control who can access the queue.


==============
QueueSet class
==============

:py:class:`dirq.queue.QueueSet` - interface to a set of Queue objects

Usage::

    from dirq.queue import Queue, QueueSet

    dq1 = Queue("/tmp/q1")
    dq2 = Queue("/tmp/q2")
    dqset = QueueSet(dq1, dq2)
    # dqs = [dq1, dq2]
    # dqset = QueueSet(dqs)

    (dq, elt) = dqset.first()
    while dq:
        # you can now process the element elt of queue dq...
        (dq, elt) = dqset.next()

Description
-----------

    This class can be used to put different queues into a set and browse
    them as one queue. The elements from all queues are merged together
    and sorted independently from the queue they belong to.

Constructor
-----------

    For the signature of the QueueSet constructor see documentation to the
    respective :py:meth:`dirq.queue.QueueSet.__init__` method.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""

import dirq
import errno
import os
import re
import sys
import time

from dirq.QueueBase import QueueBase, _DIRELT_REGEXP, _DIRECTORY_REGEXP, \
    _ELEMENT_REGEXP
from dirq.QueueBase import (
    _name,
    _special_mkdir,
    _special_rmdir,
    _file_read,
    _file_write,
    _directory_contents,
    _warn)
from dirq.Exceptions import QueueError, QueueLockError
from dirq.utils import VALID_STR_TYPES, VALID_INT_TYPES

# for backward compatibility.
from dirq.QueueSet import QueueSet

__author__ = dirq.AUTHOR
__version__ = dirq.VERSION
__date__ = dirq.DATE

__all__ = ['Queue', 'QueueSet', 'QueueError']

# name of the directory holding temporary elements
TEMPORARY_DIRECTORY = "temporary"

# name of the directory holding obsolete elements
OBSOLETE_DIRECTORY = "obsolete"

# name of the directory indicating a locked element
LOCKED_DIRECTORY = "locked"

#
# global variables
#

_FILE_REGEXP = re.compile("^([0-9a-zA-Z]+)$")
_KEY_VAL_REGEXP = re.compile("^([^\x09\x0a]*)\x09([^\x09\x0a]*)$")
_H2S_REGEXP = re.compile("(\\\\|\x09|\x0a)")
_S2H_REGEXP = re.compile(r"(\\\\|\\t|\\n)")

_BYTE2ESC = {"\\": r"\\", "\t": r"\t", "\n": r"\n"}
_ESC2BYTE = dict([(_value, _key) for _key, _value in _BYTE2ESC.items()])

#
# Helper Functions
#


def _hash2string(data):
    """Transform a hash of strings into a string.

    Raise:
        QueueError - invalid type of a value in hash
                     (allowed string or unicode)

    Note:
        the keys are sorted so that identical hashes yield to identical strings
    """
    string = ''
    for key in sorted(data.keys()):
        val = data[key]
        if type(val) not in VALID_STR_TYPES:
            raise QueueError("invalid hash value type: %r" % val)
        key = _H2S_REGEXP.sub(lambda m: _BYTE2ESC[m.group(1)], key)
        val = _H2S_REGEXP.sub(lambda m: _BYTE2ESC[m.group(1)], val)
        string = '%s%s' % (string, '%s\x09%s\x0a' % (key, val))
    return string


def _string2hash(given):
    """Transform a string into a hash of strings.

    Raise:
        QueueError - unexpected hash line

    Note:
        duplicate keys are not checked (the last one wins)
    """
    _hash = dict()
    if not given:
        return _hash
    for line in given.strip('\n').split('\x0a'):
        match = _KEY_VAL_REGEXP.match(line)
        if not match:
            raise QueueError("unexpected hash line: %s" % line)
        key = _S2H_REGEXP.sub(lambda m: _ESC2BYTE[str(m.group(1))],
                              match.group(1))
        val = _S2H_REGEXP.sub(lambda m: _ESC2BYTE[str(m.group(1))],
                              match.group(2))
        _hash[key] = val
    return _hash


def _older(path, given_time):
    """
    Check if a path is old enough:

    * return true if the path exists and is (strictly) older than given time
    * return false if it does not exist or it is newer
    * die in case of any other error

    Raise:
        OSError - can't stat given path

    Note:
        lstat() is used so symlinks are not followed
    """
    try:
        stat = os.lstat(path)
    except Exception:
        error = sys.exc_info()[1]
        if error.errno != errno.ENOENT:
            raise OSError("cannot lstat(%s): %s" % (path, error))
            # RACE: this path does not exist (anymore)
        return False
    else:
        return stat.st_mtime < given_time


def __subdirs_num_nlink(path):
    """Count the number of sub-directories in the given directory:

    * return 0 if the directory does not exist (anymore)
    * die in case of any other error

    Raise:
        OSError - can't stat given path

    Note:

    * lstat() is used so symlinks are not followed
    * this only checks the number of links
    * we do not even check that the path indeed points to a directory!
    """
    try:
        stat = os.lstat(path)
    except Exception:
        error = sys.exc_info()[1]
        if error.errno != errno.ENOENT:
            raise OSError("cannot lstat(%s): %s" % (path, error))
            # RACE: this path does not exist (anymore)
        return 0
    else:
        return stat.st_nlink - 2


def __subdirs_num_count(path):
    """Count the number of sub-directories in the given directory:
     - return 0 if the directory does not exist (anymore)

    For systems where we cannot rely on the number of links, so we simply count
    the number of sub-directories.
    """
    return len(_directory_contents(path, missingok=True))


if sys.platform in ['win32', 'cygwin']:
    _subdirs_num = __subdirs_num_count
else:
    _subdirs_num = __subdirs_num_nlink


def _check_element(name):
    """Check the given string to make sure it represents a valid element name.

    Raise:
        QueueError - given element is invalid
    """
    if not _DIRELT_REGEXP.match(name):
        raise QueueError("invalid element name: %s" % name)


def _count(path):
    """Return the number of elements in the queue, regardless of
    their state.

    Raise:
        OSError - can't list/stat element directories
    """
    count = 0
    for name in [x for x in _directory_contents(path)]:
        subdirs = _subdirs_num('%s/%s' % (path, name))
        if subdirs:
            count += subdirs
    return count

#
# Object Oriented Interface
#


class Queue(QueueBase):
    """Directory based queue.
    """
    def __init__(self, path, umask=None, rndhex=None, maxelts=16000,
                 schema=dict()):
        """Check and set schema. Build the queue directory structure.

        Arguments:
            path
                the queue toplevel directory
            umask
                the umask to use when creating files and directories
                (default: use the running process' umask)
            rndhex
                the hexadecimal digit to use in names
                (default: randomly chosen)
            maxelts
                the maximum number of elements that an intermediate
                directory can hold (default: 16,000)
            schema
                the schema defining how to interpret user supplied
                data (mandatory if elements are added or read)
        Raise:
            TypeError  - wrong input data types provided
            QueueError - problems with the queue schema definition
            OSError    - can't create directory structure
        """
        super(Queue, self).__init__(path, umask=umask, rndhex=rndhex)

        if type(maxelts) in VALID_INT_TYPES:
            self.maxelts = maxelts
        else:
            raise TypeError("'maxelts' should be int or long")
        # check schema
        self.type = {}
        self.mandatory = {}
        if schema:
            if not isinstance(schema, dict):
                raise QueueError("invalid schema: %r" % schema)
            for name in schema.keys():
                if not _FILE_REGEXP.match(name):
                    raise QueueError("invalid schema name: %r" % name)
                if not isinstance(schema[name], str):
                    raise QueueError("invalid data type for schema " +
                                     "specification: %r" % type(schema[name]))
                match = re.match('(binary|string|table)([\\?\\*]{0,2})?$',
                                 schema[name])
                if not match:
                    raise QueueError("invalid schema data type: %r" %
                                     schema[name])
                self.type[name] = match.group(1)
                if not re.search('\\?', match.group(2)):
                    self.mandatory[name] = True
            if not self.mandatory:
                raise QueueError("invalid schema: no mandatory data")
        # create directories
        for directory in (TEMPORARY_DIRECTORY, OBSOLETE_DIRECTORY):
            _special_mkdir('%s/%s' % (self.path, directory), self.umask)

    def _is_locked_nlink(self, ename, _time=None):
        """Check if an element is locked.

        Note:
            this is only an indication as the state may be changed by another
            process

        Uses number of links (st_nlink) returned by os.lstat() applied to the
        element directory.

        Arguments:
            ename - name of an element
            _time - consider only locks older than the given time

        Return:
            True  - if element exists and locked. If _time is provided, only
            return True on locks older than this time (needed by purge).

            False - in other cases

        Raises:
            OSError - if unable to stat() the element
        """
        path = '%s/%s' % (self.path, ename)
        try:
            stat = os.lstat(path)
        except Exception:
            error = sys.exc_info()[1]
            if error.errno != errno.ENOENT:
                raise OSError("cannot lstat(%s): %s" % (path, error))
            return False
        # locking increases number of links
        if not stat.st_nlink > 2:
            return False
        # check age if _time is given
        if _time and stat.st_mtime >= _time:
            return False
        return os.path.exists('%s/%s' % (path, LOCKED_DIRECTORY))

    def _is_locked_nonlink(self, ename, _time=None):
        """See _is_locked_nlink(). This version doesn't use nlink (slower).
        """
        path = '%s/%s' % (self.path, ename)
        if not os.path.exists('%s/%s' % (path, LOCKED_DIRECTORY)):
            return False
        elif not _time:
            return True
        # element exists and locked, and we were asked to act upon its age
        try:
            stat = os.lstat(path)
        except Exception:
            error = sys.exc_info()[1]
            if error.errno != errno.ENOENT:
                raise OSError("cannot lstat(%s): %s" % (path, error))
            return False
        return stat.st_mtime < _time

    if sys.platform in ['win32', 'cygwin']:
        _is_locked = _is_locked_nonlink
    else:
        _is_locked = _is_locked_nlink

    def _build_elements(self):
        """Build list of elements.

        Raise:
            OSError - can't list element directories
        """
        while self.dirs:
            directory = self.dirs.pop(0)
            _list = []
            for name in _directory_contents(
                    '%s/%s' % (self.path, directory), True):
                if _ELEMENT_REGEXP.match(name):
                    _list.append(name)
            if not _list:
                continue
            self.elts = ['%s/%s' % (directory, x) for x in sorted(_list)]
            return True
        return False

    def count(self):
        """Return the number of elements in the queue, regardless of
        their state.

        Raise:
            OSError - can't list/stat element directories
        """
        return _count(self.path)

    def lock(self, ename, permissive=True):
        """Lock an element.

        Arguments:
            ename - name of an element
            permissive - work in permissive mode

        Return:

        * True on success
        * False in case the element could not be locked (in permissive
          mode)

        Raise:
            QueueError - invalid element name
            OSError    - can't create lock (mkdir()/lstat() failed)

        Note:

        * locking can fail:

          * if the element has been locked by somebody else (EEXIST)
          * if the element has been removed by somebody else (ENOENT)
        * if the optional second argument is true, it is not an error if
          the element cannot be locked (permissive mode), this is the
          default
        * the directory's mtime will change automatically (after a
          successful mkdir()), this will later be used to detect stalled
          locks
        """
        _check_element(ename)
        path = '%s/%s/%s' % (self.path, ename, LOCKED_DIRECTORY)
        try:
            if self.umask is not None:
                oldumask = os.umask(self.umask)
                os.mkdir(path)
                os.umask(oldumask)
            else:
                os.mkdir(path)
            os.lstat(path)
        except Exception:
            error = sys.exc_info()[1]
            if permissive:
                # RACE: the locked directory already exists
                if error.errno == errno.EEXIST:
                    return False
                # RACE: the element directory does not exist anymore
                if error.errno == errno.ENOENT:
                    return False
            # otherwise this is unexpected...
            raise OSError("cannot mkdir(%s): %s" % (path, error))
        try:
            os.lstat(path)
        except Exception:
            error = sys.exc_info()[1]
            if permissive:
                # RACE: the element directory does not exist anymore (this can
                # happen if an other process locked & removed the element
                # while our mkdir() was in progress... yes, this can happen!)
                if error.errno == errno.ENOENT:
                    return False
            # otherwise this is unexpected...
            raise OSError("cannot lstat(%s): %s" % (path, str(error)))
        return True

    def unlock(self, ename, permissive=False):
        """Unlock an element.

        Arguments:
            ename - name of an element
            permissive - work in permissive mode

        Return:

        * true on success
        * false in case the element could not be unlocked (in permissive
          mode)

        Raise:
            QueueError - invalid element name
            OSError    - can't remove lock (rmdir() failed)

        Note:

        * unlocking can fail:

            * if the element has been unlocked by somebody else (ENOENT)
            * if the element has been removed by somebody else (ENOENT)
        * if the optional second argument is true, it is not an error if
          the element cannot be unlocked (permissive mode), this is _not_
          the default
        """
        _check_element(ename)
        path = '%s/%s/%s' % (self.path, ename, LOCKED_DIRECTORY)
        try:
            os.rmdir(path)
        except Exception:
            error = sys.exc_info()[1]
            if permissive:
                # RACE: the element folder or its lock does not exist anymore
                if error.errno == errno.ENOENT:
                    return False
            raise OSError("cannot rmdir(%s): %s" % (path, error))
        else:
            return True

    def remove(self, ename):
        """Remove locked element from the queue.

        Arguments:
            ename - name of an element

        Raise:
            QueueError - invalid element name; element not locked;
                         unexpected file in the element directory

            OSError    - can't rename/remove a file/directory

        Note:
            doesn't return anything explicitly (i.e. returns NoneType)
            or fails
        """
        _check_element(ename)
        if not self._is_locked(ename):
            raise QueueError("cannot remove %s: not locked" % ename)
        # move the element out of its intermediate directory
        path = '%s/%s' % (self.path, ename)
        while True:
            temp = '%s/%s/%s' % (self.path, OBSOLETE_DIRECTORY,
                                 _name(self.rndhex))
            try:
                os.rename(path, temp)
                break
            except Exception:
                error = sys.exc_info()[1]
                if error.errno != errno.ENOTEMPTY and \
                        error.errno != errno.EEXIST:
                    raise OSError("cannot rename(%s, %s): %s" %
                                  (ename, temp, error))
                    # RACE: the target directory was already present...
        # remove the data files
        for name in _directory_contents(temp):
            if name == LOCKED_DIRECTORY:
                continue
            if not _FILE_REGEXP.match(name):
                raise QueueError("unexpected file in %s: %s" % (temp, name))
            path = '%s/%s' % (temp, name)
            try:
                os.unlink(path)
            except Exception:
                error = sys.exc_info()[1]
                raise OSError("cannot unlink(%s): %s" % (path, error))
        # remove the locked directory
        path = '%s/%s' % (temp, LOCKED_DIRECTORY)
        while True:
            try:
                os.rmdir(path)
            except Exception:
                error = sys.exc_info()[1]
                raise OSError("cannot rmdir(%s): %s" % (path, error))
            try:
                os.rmdir(temp)
                return
            except Exception:
                error = sys.exc_info()[1]
                if error.errno != errno.ENOTEMPTY and \
                        error.errno != errno.EEXIST:
                    raise OSError("cannot rmdir(%s): %s" % (temp, error))
                # RACE: this can happen if an other process managed to lock
                # this element while it was being removed (see the comment in
                # the lock() method) so we try to remove the lock again
                # and again...

    def dequeue(self, ename, permissive=True):
        """Dequeue an element from the queue. Removes element from the
        queue. Performs operations: lock(name), get(name), remove(name)

        Arguments:
            ename - name of an element

        Return:
            dictionary representing an element

        Raise:
            QueueLockError - coulnd't lock element
            QueueError     - problems with schema/data types/etc.
            OSError        - problems opening/closing directory/file
        """
        if not self.lock(ename, permissive=permissive):
            raise QueueLockError("couldn't lock element: %s" % ename)
        element = self.get(ename)
        self.remove(ename)
        return element

    def get(self, ename):
        """Get an element data from a locked element.

        Arguments:
            ename - name of an element

        Return:
            dictionary representing an element

        Raise:
            QueueError - schema is unknown; unexpected data type in
                         the schema specification; missing mandatory
                         file of the element

            OSError    - problems opening/closing file
            IOError    - file read error
        """
        if not self.type:
            raise QueueError("unknown schema")
        _check_element(ename)
        if not self._is_locked(ename):
            raise QueueError("cannot get %s: not locked" % ename)
        data = {}
        for dname in self.type.keys():
            path = '%s/%s/%s' % (self.path, ename, dname)
            try:
                os.lstat(path)
            except Exception:
                error = sys.exc_info()[1]
                if error.errno != errno.ENOENT:
                    raise OSError("cannot lstat(%s): %s" % (path, error))
                if dname in self.mandatory:
                    raise QueueError("missing data file: %s" % path)
                else:
                    continue
            if self.type[dname] == 'binary':
                data[dname] = _file_read(path, 0)
            elif self.type[dname] == 'string':
                data[dname] = _file_read(path, 1)
            elif self.type[dname] == 'table':
                data[dname] = _string2hash(_file_read(path, 1))
            else:
                raise QueueError("unexpected data type: %s" % self.type[dname])
        return data

    def get_element(self, ename, permissive=True):
        """Get an element from the queue. Element will not be removed.
        Operations performed: lock(name), get(name), unlock(name)

        Arguments:
            ename - name of an element

        Raise:
            QueueLockError - couldn't lock element
        """
        if not self.lock(ename, permissive=permissive):
            raise QueueLockError("couldn't lock element: %s" % ename)
        element = self.get(ename)
        self.unlock(ename, permissive=permissive)
        return element

    def _insertion_directory(self):
        """Return the name of the intermediate directory that can be used for
        insertion:

        * if there is none, an initial one will be created
        * if it is full, a new one will be created
        * in any case the name will match $_DIRECTORY_REGEXP

        Raise:
            OSError - can't list/make element directories
        """
        _list = []
        # get the list of existing directories
        for name in _directory_contents(self.path):
            if _DIRECTORY_REGEXP.match(name):
                _list.append(name)
        # handle the case with no directories yet
        if not _list:
            name = '%08x' % 0
            _special_mkdir('%s/%s' % (self.path, name), self.umask)
            return name
        # check the last directory
        _list.sort()
        name = _list[-1]
        subdirs = _subdirs_num('%s/%s' % (self.path, name))
        if subdirs:
            if subdirs < self.maxelts:
                return name
        else:
            # RACE: at this point, the directory does not exist anymore,
            # so it must have been purged after we listed the directory
            # contents. We do not try to do more and simply create a new
            # directory
            pass
        # we need a new directory
        name = '%08x' % (int(name, 16) + 1)
        _special_mkdir('%s/%s' % (self.path, name), self.umask)
        return name

    def add(self, data):
        """Add a new element to the queue and return its name.
        Arguments:

            data - element as a dictionary (should conform to the schema)

        Raise:

            QueueError - problem with schema definition or data
            OSError    - problem putting element on disk

        Note:

          the destination directory must _not_ be created beforehand as
          it would be seen as a valid (but empty) element directory by
          another process, we therefore use rename() from a temporary
          directory
        """
        if not self.type:
            raise QueueError("unknown schema")
        while True:
            temp = '%s/%s/%s' % (self.path, TEMPORARY_DIRECTORY,
                                 _name(self.rndhex))
            if _special_mkdir(temp, self.umask):
                break
        for name in data.keys():
            if name not in self.type:
                raise QueueError("unexpected data: %s" % name)
            if self.type[name] == 'binary':
                if type(data[name]) not in VALID_STR_TYPES:
                    raise QueueError("unexpected binary data in %s: %r" %
                                     (name, data[name]))
                _file_write('%s/%s' % (temp, name), 0, self.umask, data[name])
            elif self.type[name] == 'string':
                if type(data[name]) not in VALID_STR_TYPES:
                    raise QueueError("unexpected string data in %s: %r" %
                                     (name, data[name]))
                _file_write('%s/%s' % (temp, name), 1, self.umask, data[name])
            elif self.type[name] == 'table':
                if not isinstance(data[name], dict):
                    raise QueueError("unexpected table data in %s: %r" %
                                     (name, data[name]))
                _file_write('%s/%s' % (temp, name), 1, self.umask,
                            _hash2string(data[name]))
            else:
                raise QueueError("unexpected data type in %s: %r" %
                                 (name, self.type[name]))
        for name in self.mandatory.keys():
            if name not in data:
                raise QueueError("missing mandatory data: %s" % name)
        while True:
            name = '%s/%s' % (self._insertion_directory(), _name(self.rndhex))
            path = '%s/%s' % (self.path, name)
            try:
                os.rename(temp, path)
                return name
            except Exception:
                error = sys.exc_info()[1]
                if error.errno != errno.ENOTEMPTY and \
                        error.errno != errno.EEXIST:
                    raise OSError("cannot rename(%s, %s): %s" %
                                  (temp, path, error))
                    # RACE: the target directory was already present...
    enqueue = add

    def _volatile(self):
        """
        Return the list of volatile (i.e. temporary or obsolete) directories.
        """
        _list = []
        for name in _directory_contents(
                '%s/%s' % (self.path, TEMPORARY_DIRECTORY), True):
            if _ELEMENT_REGEXP.match(name):
                _list.append('%s/%s' % (TEMPORARY_DIRECTORY, name))
        for name in _directory_contents(
                '%s/%s' % (self.path, OBSOLETE_DIRECTORY), True):
            if _ELEMENT_REGEXP.match(name):
                _list.append('%s/%s' % (OBSOLETE_DIRECTORY, name))
        return _list

    def purge(self, maxtemp=300, maxlock=600):
        """Purge the queue:

        * delete unused intermediate directories
        * delete too old temporary directories
        * unlock too old locked directories

        Arguments:
            maxtemp - maximum time for a temporary element. If 0, temporary
                      elements will not be removed.
            maxlock - maximum time for a locked element. If 0, locked
                      elements will not be unlocked.
        Raise:
            OSError - problem deleting element from disk

        Note:
            this uses first()/next() to iterate so this will reset the cursor
        """
        # get the list of intermediate directories
        _list = []
        for name in _directory_contents(self.path):
            if _DIRECTORY_REGEXP.match(name):
                _list.append(name)
        _list.sort()
        # try to purge all but last one
        if len(_list) > 1:
            _list.pop()
            for name in _list:
                path = '%s/%s' % (self.path, name)
                if _subdirs_num(path):
                    continue
                _special_rmdir(path)
        # remove the volatile directories which are too old
        if maxtemp:
            oldtime = time.time() - maxtemp
            for name in self._volatile():
                path = '%s/%s' % (self.path, name)
                if _older(path, oldtime):
                    _warn("* removing too old volatile element: %s" % name)
                    for file_name in _directory_contents(path, True):
                        if file_name == LOCKED_DIRECTORY:
                            continue
                        fpath = '%s/%s' % (path, file_name)
                        try:
                            os.unlink(fpath)
                        except Exception:
                            error = sys.exc_info()[1]
                            if error.errno != errno.ENOENT:
                                raise OSError("cannot unlink(%s): %s" %
                                              (fpath, error))
                _special_rmdir('%s/%s' % (path, LOCKED_DIRECTORY))
                _special_rmdir(path)
        # iterate to find abandoned locked entries
        if maxlock:
            oldtime = time.time() - maxlock
            name = self.first()
            while name:
                if self._is_locked(name, oldtime):
                    _warn("* removing too old locked element: %s" % name)
                    self.unlock(name, True)
                name = self.next()
