"""
Directory based queue.

A port of Perl module Directory::Queue
http://search.cpan.org/dist/Directory-Queue/
The documentation from Directory::Queue module was adapted for Python.

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue.

PROVIDES

Classes:
- Queue      directory based queue
- QueueSet   set of directory based queues
- QueueError exception

DOCUMENTATION

===================
=== Queue class ===

Queue - directory based queue.

USAGE
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
        print "# added element %i as %s" %(count, name)

    # sample consumer

    dirq = Queue(queuedir, schema=schema)
    name = dirq.first()
    while name:
        if not dirq.lock(name):
            name = dirq.next()
            continue
        print "# reading element %s" % name
        data = dirq.get(name)
        # one can use data['body'] and data['header'] here...
        # one could use dirq.unlock(name) to only browse the queue...
        dirq.remove(name)
        name = dirq.next()

TERMINOLOGY
    An element is something that contains one or more pieces of data. A
    simple string may be an element but more complex schemas can also be
    used, see the "SCHEMA" section for more information.

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

LOCKING
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

CONSTRUCTOR
    For the signature of the Queue constructor see documentation to the
    respective __init__() method.

SCHEMA
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
    the comments in the "SYNOPSIS" section for more information.

DIRECTORY STRUCTURE
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

SECURITY
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

======================
=== QueueSet class ===

QueueSet - interface to a set of Queue objects

USAGE
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

DESCRIPTION
    This class can be used to put different queues into a set and browse
    them as one queue. The elements from all queues are merged together
    and sorted independently from the queue they belong to.

CONSTRUCTOR
    For the signature of the QueueSet constructor see documentation to the
    respective __init__() method.

----------------------------

AUTHOR

Konstantin Skaburskas

LICENSE AND COPYRIGHT

ASL 2.0

Copyright (C) 2010 CERN
"""

import os
import sys
import time
import errno
import re
import inspect
import codecs

__all__ = ['Queue','QueueSet','QueueError']

# stat(2) fields
ST_NLINK = 3
ST_MTIME = 9

# name of the directory holding temporary elements
TEMPORARY_DIRECTORY = "temporary"

# name of the directory holding obsolete elements
OBSOLETE_DIRECTORY = "obsolete"

# name of the directory indicating a locked element
LOCKED_DIRECTORY = "locked"

# states returned by _state()
STATE_UNLOCKED = "U"
STATE_LOCKED   = "L"
STATE_MISSING  = "M"

#
# global variables
#

__DirectoryRegexp = '[0-9a-f]{8}'
_DirectoryRegexp  = re.compile('(%s)$' % __DirectoryRegexp)
__ElementRegexp   = '[0-9a-f]{14}'
_ElementRegexp    = re.compile('(%s)$' % __ElementRegexp)
_DirElemRegexp    = re.compile('^%s/%s$'%(__DirectoryRegexp,
                                          __ElementRegexp))
__FileRegexp      = "[0-9a-zA-Z]+"
_FileRegexp       = re.compile("^(%s)$" % __FileRegexp)
_KeyValRegexp     = re.compile('^([^\x09\x0a]*)\x09([^\x09\x0a]*)$')

_Byte2Esc = {"\x5c" : "\\", "\x09" : "\\t", "\x0a" : "\\n"}
_Esc2Byte = dict([(v,k) for k, v in _Byte2Esc.iteritems()])

UPID = '%01x' % (os.getpid() % 16)

WARN = False

#
# Helper Functions
#

def _warn(text):
    if WARN:
        sys.stdout.write('%s, at %s line %s\n' % (text, __name__,
                                        inspect.currentframe().f_back.f_lineno))

def _file_read(path, utf8):
    """Read from a file.
    Raise:
    OSError - problems opening/closing file
    IOError - file read error
    """
    try:
        if utf8:
            fh = codecs.open(path, 'r', "utf8")
        else:
            fh = open(path, 'rb')
    except StandardError, e:
        raise OSError("cannot open %s: %s"%(path, str(e)))
    try:
        contents = fh.read()
    except StandardError, e:
        raise IOError("cannot read %s: %s"%(path, str(e)))
    try:
        fh.close()
    except StandardError, e:
        raise OSError("cannot close %s: %s"%(path, str(e)))
    return contents

def _file_write(path, utf8, umask, contents):
    """Write to a file.
    Raise:
    OSError - problems opening/closing file
    IOError - file write error
    """
    try:
        if umask:
            oldumask = os.umask(umask)
        if utf8:
            fh = codecs.open(path, 'w', 'utf8')
        else:
            fh = open(path, 'wb')
        if umask:
            os.umask(oldumask)
    except StandardError, e:
        raise OSError("cannot open %s: %s"%(path, str(e)))
    try:
        fh.write(contents)
    except StandardError, e:
        raise IOError("cannot write to %s: %s"%(path, str(e)))
    try:
        fh.close()
    except StandardError, e:
        raise OSError("cannot close %s: %s"%(path, str(e)))

def _hash2string(hash):
    """Transform a hash of strings into a string.

    Raise:
    QueueError - invalid type of a value in hash (allowed string or unicode)
    note:
     - the keys are sorted so that identical hashes yield to identical strings
    """
    string = ''
    for key in sorted(hash.keys()):
        val = hash[key]
        if type(val) not in [str, unicode]:
            raise QueueError("invalid hash value type: %r"%val)
        key = re.sub('([\x5c\x09\x0a])', lambda m: _Byte2Esc[m.group(1)], key)
        val = re.sub('([\x5c\x09\x0a])', lambda m: _Byte2Esc[m.group(1)], val)
        string = '%s%s' % (string, '%s\x09%s\x0a'%(key,val))
    return string

def _string2hash(string):
    """Transform a string into a hash of strings.
    Raise:
    QueueError - unexpected hash line
    note:
     - duplicate keys are not checked (the last one wins)
    """
    _hash = {}
    for line in string.strip('\n').split('\x0a'):
        match = _KeyValRegexp.match(line)
        if not match:
            raise QueueError("unexpected hash line: %s"%line)
        key = re.sub('(\\\\|\\t|\\n)', lambda m: _Esc2Byte[str(m.group(1))],
                     match.group(1))
        val = re.sub('(\\\\|\\t|\\n)', lambda m: _Esc2Byte[str(m.group(1))],
                     match.group(2))
        _hash[key] = val
        _hash[match.group(1)] = match.group(2)
    return _hash

def _directory_contents(path, missingok=True):
    """Get the contents of a directory as a list of names, without . and ..
    Raise:
    OSError - can't list directory
    note:
     - if the optional second argument is true, it is not an error if the
       directory does not exist (anymore)
    """
    try:
        return os.listdir(path)
    except StandardError, e:
        if not missingok and not e.errcode == errno.ENOENT:
            raise OSError("cannot listdir(%s): %s"%(path, str(e)))
            # RACE: this path does not exist (anymore)
        return []

def _older(path, time):
    """Check if a path is old enough:
     - return true if the path exists and is (strictly) older than given time
     - return false if it does not exist or it is newer
     - die in case of any other error
    Raise:
    OSError - can't stat given path
    note:
     - lstat() is used so symlinks are not followed
    """
    try:
        stat = os.lstat(path)
    except StandardError, e:
        if not e.errno == errno.ENOENT:
            raise OSError("cannot lstat(%s): %s"%(path, str(e)))
            # RACE: this path does not exist (anymore)
        return False
    else:
        return stat.st_mtime < time

def __subdirs_num(path):
    """Count the number of sub-directories in the given directory:
     - return 0 if the directory does not exist (anymore)
     - die in case of any other error

    Raise:
    OSError - can't stat given path
    note:
     - lstat() is used so symlinks are not followed
     - this only checks the number of links
     - we do not even check that the path indeed points to a directory!
    """
    try:
        stat = os.lstat(path)
    except StandardError, e:
        if not e.errno == errno.ENOENT:
            raise OSError("cannot lstat(%s): %s"%(path, str(e)))
            # RACE: this path does not exist (anymore)
        return 0
    else:
        return stat.st_nlink - 2

def __subdirs_num_Windows(path):
    """Count the number of sub-directories in the given directory:
     - return 0 if the directory does not exist (anymore)

    Windows version where we simply count number of sub-directories as we
    cannot rely on the number of links.
    """
    return len(_directory_contents(path, missingok=True))

if sys.platform in ['win32', 'cygwin']:
    _subdirs_num = __subdirs_num_Windows
else:
    _subdirs_num = __subdirs_num

def _special_mkdir(path, umask=None):
    """Create a directory:
     - return true on success
     - return false if something with the same path already exists
     - die in case of any other error

    Raise:
    OSError - can't make directory
    note:
     - in case something with the same path already exists, we do not check
       that this is indeed a directory as this should always be the case here
    """
    try:
        if umask != None:
            print umask
            oldumask = os.umask(umask)
            os.mkdir(path)
            os.umask(oldumask)
        else:
            os.mkdir(path)
    #except (OSError, IOError), e:
    except EnvironmentError, e:
        if e.errno == errno.EEXIST or e.errno == errno.EISDIR:
            return False
        else:
            raise OSError("cannot mkdir(%s): %s"%(path, str(e)))
    else:
        return True

def _special_rmdir(path):
    """Delete a directory:
     - return true on success
     - return false if the path does not exist (anymore)
     - die in case of any other error
     Raise:
     OSError - can't delete given directory
    """
    try:
        os.rmdir(path)
    except StandardError, e:
        if not e.errno == errno.ENOENT:
            raise OSError("cannot rmdir(%s): %s"%(path, str(e)))
            # RACE: this path does not exist (anymore)
        return False
    else:
        return True

def _new_name():
    """Return the name of a new element to (try to) use with:
     - 8 hexadecimal digits for the number of seconds since the Epoch
     - 5 hexadecimal digits for the microseconds part
     - 1 hexadecimal digit from the pid to further reduce name collisions

    properties:
     - fixed size (14 hexadecimal digits)
     - likely to be unique (with high-probability)
     - can be lexically sorted
     - ever increasing (for a given process)
     - reasonably compact
     - matching $_ElementRegexp
    """
    t = time.time()
    return "%08x%05x%s" % (t, (t % 1.0)*100000, UPID)

def _check_element(name):
    """Check the given string to make sure it represents a valid element name.
    Raise:
    QueueError - given element is invalid
    """
    if not _DirElemRegexp.match(name):
        raise QueueError("invalid element name: %s"%name)

def _count(path):
    """Return the number of elements in the queue, regardless of
    their state.

    Raise:
    OSError - can't list/stat element directories
    """
    count = 0
    for name in [x for x in _directory_contents(path)]:
        subdirs = _subdirs_num('%s/%s'%(path,name))
        if subdirs:
            count += subdirs
    return count

#
# Object Oriented Interface
#

class QueueError(Exception):
    ''

class QueueLockError(QueueError):
    ''

class Queue(object):
    """Directory based queue.
    """
    def __init__(self, path, umask=None, maxelts=16000, schema={}):
        """Check and set schema. Build the queue directory structure.
        Arguments:
            path
                the queue toplevel directory
            umask
                the umask to use when creating files and directories
                (default: use the running process' umask)
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
        self.dirs = []
        self.elts = []
        self._next_exception = False

        if type(path) not in [str, unicode]:
            raise TypeError("'path' should be str or unicode")
        self.path = path
        if umask != None or isinstance(umask, int):
            raise TypeError("'umask' should be integer")
        self.umask = umask
        if type(maxelts) in [int, long]:
            self.maxelts = maxelts
        else:
            raise TypeError("'maxelts' should be int or long")
        # check schema
        self.type = {}
        self.mandatory = {}
        if schema:
            if not isinstance(schema, dict):
                raise QueueError("invalid schema: %r"%schema)
            for name in schema.keys():
                if not _FileRegexp.match(name):
                    raise QueueError("invalid schema name: %r"%name)
                if not isinstance(schema[name], str):
                    raise QueueError("invalid data type for schema "+\
                                    "specification: %r"%type(schema[name]))
                m = re.match('(binary|string|table)(\?)?$', schema[name])
                if not m:
                    raise QueueError("invalid schema data type: %r"%schema[name])
                self.type[name] = m.group(1)
                if not m.group(2):
                    self.mandatory[name] = True
            if not self.mandatory:
                raise QueueError("invalid schema: no mandatory data")
        # create top level directory
        path = ''
        for d in self.path.split('/'):
            path = '%s/%s' % (path, d)
            _special_mkdir(path, self.umask)
        # create other directories
        for d in (TEMPORARY_DIRECTORY, OBSOLETE_DIRECTORY):
            _special_mkdir('%s/%s'%(self.path,d), self.umask)
        # store the queue unique identifier
        if sys.platform in ['win32']:
            self.id = self.path
        else:
            stat = os.stat(self.path)
            self.id = '%s:%s' % (stat.st_dev, stat.st_ino)

    def __iter__(self):
        """Return iterator over element names.
        """
        self._reset()
        self._next_exception = True
        return self

    def names(self):
        """Return iterator over element names.
        """
        return self.__iter__()

    def copy(self):
        """Copy/clone the object. Return copy of the object.

        note:
         - the main purpose is to copy/clone the iterator cached state
         - the other structured attributes (including schema) are not cloned
        """
        import copy
        c = copy.deepcopy(self)
        c.dirs = []
        c.elts = []
        return c

    def _state(self, ename):
        """Return the state of the given element.
        Arguments:
            ename - name of an element
        note:
         - this is only an indication as the state may be changed by another
           process
        """
        path = '%s/%s' % (self.path, ename)
        if os.path.exists('%s/%s' % (path, LOCKED_DIRECTORY)):
            return STATE_LOCKED
        if os.path.exists(path):
            return STATE_UNLOCKED
        # the element does not exist (anymore)
        return STATE_MISSING

    def _build_elements(self):
        """Build list of elements.
        Raise:
            OSError - can't list element directories
        """
        while self.dirs:
            dir = self.dirs.pop(0)
            self.elts = []
            for name in _directory_contents('%s/%s'%(self.path,dir), True):
                if _ElementRegexp.match(name):
                    self.elts.append(name)
            if not self.elts:
                continue
            self.elts = ['%s/%s'%(dir, x) for x in sorted(self.elts)]
            if self.elts:
                return

    def next(self):
        """Return name of the next element in the queue, only using cached
        information. When queue is empty, depending on the iterator
        protocol - return empty string or raise StopIteration.
        Return:
            name of the next element in the queue
        Raise:
            StopIteration - when used as Python iterator via
                            __iter__() method
            OSError       - can't list element directories
        """
        if self.elts:
            return self.elts.pop(0)
        self._build_elements()
        if self.elts:
            return self.elts.pop(0)
        if self._next_exception:
            self._next_exception = False
            raise StopIteration
        else:
            return ''

    def _reset(self):
        """Regenerate list of intermediate directories. Drop cached
        elements list.
        Raise:
            OSError - can't list directories
        """
        self.dirs = []
        for name in _directory_contents(self.path):
            if _DirectoryRegexp.match(name):
                self.dirs.append(name)
        self.dirs.sort()
        self.elts = []

    def first(self):
        """Return the first element in the queue and cache information about
        the next ones.
        Raise:
            OSError - can't list directories
        """
        self._reset()
        return self.next()

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
         - true on success
         - false in case the element could not be locked (in permissive
           mode)
        Raise:
            QueueError - invalid element name
            OSError    - can't create lock (mkdir()/lstat() failed)
        note:
         - locking can fail:
            - if the element has been locked by somebody else (EEXIST)
            - if the element has been removed by somebody else (ENOENT)
         - if the optional second argument is true, it is not an error if
           the element cannot be locked (permissive mode), this is the
           default
         - the directory's mtime will change automatically (after a
           successful mkdir()), this will later be used to detect stalled
           locks
        """
        _check_element(ename)
        path = '%s/%s/%s' % (self.path, ename, LOCKED_DIRECTORY)
        try:
            if self.umask != None:
                oldumask = os.umask(self.umask)
                os.mkdir(path)
                os.umask(oldumask)
            else:
                os.mkdir(path)
            os.lstat(path)
        except StandardError, e:
            if permissive:
                # RACE: the locked directory already exists
                if e.errno == errno.EEXIST:
                    return False
                # RACE: the element directory does not exist anymore
                if e.errno == errno.ENOENT:
                    return False
            # otherwise this is unexpected...
            raise OSError("cannot mkdir(%s): %s"%(path, str(e)))
        try:
            os.lstat(path)
        except StandardError, e:
            if permissive:
                # RACE: the element directory does not exist anymore (this can
                # happen if an other process locked & removed the element)
                if e.errno == errno.ENOENT:
                    return False
            # otherwise this is unexpected...
            raise OSError("cannot lstat(%s): %s"%(path, str(e)))
        return True

    def unlock(self, ename, permissive=False):
        """Unlock an element.
        Arguments:
            ename - name of an element
            permissive - work in permissive mode
        Return:
         - true on success
         - false in case the element could not be unlocked (in permissive
         mode)
        Raise:
            QueueError - invalid element name
            OSError    - can't remove lock (rmdir() failed)
        note:
         - unlocking can fail:
            - if the element has been unlocked by somebody else (ENOENT)
            - if the element has been removed by somebody else (ENOENT)
         - if the optional second argument is true, it is not an error if
           the element cannot be unlocked (permissive mode), this is _not_
           the default
        """
        _check_element(ename)
        path = '%s/%s/%s' % (self.path, ename, LOCKED_DIRECTORY)
        try:
            os.rmdir(path)
        except StandardError, e:
            if permissive:
                # RACE: the element directory or its lock does not exist anymore
                if e.errno == errno.ENOENT:
                    return False
            raise OSError("cannot rmdir(%s): %s"%(path, str(e)))
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
        note:
         - doesn't return anything explicitly (i.e. returns NoneType)
           or fails
        """
        _check_element(ename)
        if self._state(ename) != STATE_LOCKED:
            raise QueueError("cannot remove %s: not locked"%ename)
        # move the element out of its intermediate directory
        path = '%s/%s' % (self.path, ename)
        while True:
            temp = '%s/%s/%s' % (self.path, OBSOLETE_DIRECTORY, _new_name())
            try:
                os.rename(path, temp)
                break
            except StandardError, e:
                if e.errno != errno.ENOTEMPTY or e.errno != errno.EEXIST:
                    raise OSError("cannot rename(%s, %s): %s"%(ename, temp,
                                                               str(e)))
                    # RACE: the target directory was already present...
        # remove the data files
        for name in _directory_contents(temp):
            if name == LOCKED_DIRECTORY:
                continue
            if not _FileRegexp.match(name):
                raise QueueError("unexpected file in %s: %s"%(temp, name))
            path = '%s/%s' % (temp, name)
            try:
                os.unlink(path)
            except StandardError, e:
                raise OSError("cannot unlink(%s): %s"%(path, str(e)))
        # remove the locked directory
        path = '%s/%s' % (temp, LOCKED_DIRECTORY)
        while True:
            try:
                os.rmdir(path)
            except StandardError, e:
                raise OSError("cannot rmdir(%s): %s"%(path, str(e)))
            try:
                os.rmdir(temp)
                return
            except Exception, e:
                if e.errno != errno.ENOTEMPTY or e.errno != errno.EEXIST:
                    raise OSError("cannot rmdir(%s): %s"%(path, str(e)))
                # RACE: this can happen if an other process managed to lock
                # this element while it was being renamed so we try again
                # to remove the lock

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
        e = self.get(ename)
        self.remove(ename)
        return e

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
        if self._state(ename) != STATE_LOCKED:
            raise QueueError("cannot get %s: not locked"%ename)
        data = {}
        for dname in self.type.keys():
            path = '%s/%s/%s' % (self.path, ename, dname)
            try:
                os.lstat(path)
            except StandardError, e:
                if e.errno != errno.ENOENT:
                    raise OSError("cannot lstat(%s): %s"%(path, str(e)))
                if self.mandatory.has_key(dname):
                    raise QueueError("missing data file: %s"%path)
                else:
                    continue
            if self.type[dname] == 'binary':
                data[dname] = _file_read(path, 0)
            elif self.type[dname] == 'string':
                data[dname] = _file_read(path, 1)
            elif self.type[dname] == 'table':
                data[dname] = _string2hash(_file_read(path, 1))
            else:
                raise QueueError("unexpected data type: %s"%self.type[dname])
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
        e = self.get(ename)
        self.unlock(ename, permissive=permissive)
        return e

    def _insertion_directory(self):
        """Return the name of the intermediate directory that can be used for
        insertion:
        - if there is none, an initial one will be created
        - if it is full, a new one will be created
        - in any case the name will match $_DirectoryRegexp
        Raise:
            OSError - can't list/make element directories
        """
        _list = []
        # get the list of existing directories
        for name in _directory_contents(self.path):
            if _DirectoryRegexp.match(name):
                _list.append(name)
        # handle the case with no directories yet
        if not _list:
            name = '%08x' % 0
            _special_mkdir('%s/%s'%(self.path,name), self.umask)
            return name
        # check the last directory
        _list.sort()
        name = _list[-1]
        subdirs = _subdirs_num('%s/%s'%(self.path,name))
        if subdirs:
            if subdirs < self.maxelts:
                return name
        else:
            """RACE: at this point, the directory does not exist anymore,
            so it must have been purged after we listed the directory
            contents. We do not try to do more and simply create a new
            directory"""
        # we need a new directory
        name = '%08x' % (int(name, 16) + 1)
        _special_mkdir('%s/%s'%(self.path,name), self.umask)
        return name

    def add(self, data):
        """Add a new element to the queue and return its name.
        Arguments:
            data - element as a dictionary (should conform to the schema)
        Raise:
            QueueError - problem with schema definition or data
            OSError    - problem putting element on disk
        note:
        - the destination directory must _not_ be created beforehand as
          it would be seen as a valid (but empty) element directory by
          another process, we therefore use rename() from a temporary
          directory
        """
        if not self.type:
            raise QueueError("unknown schema")
        while True:
            temp = '%s/%s/%s' % (self.path, TEMPORARY_DIRECTORY, _new_name())
            if _special_mkdir(temp, self.umask):
                break
        for name in data.keys():
            if not self.type.has_key(name):
                raise QueueError("unexpected data: %s"%name)
            if self.type[name] == 'binary':
                if type(data[name]) not in [str, unicode]:
                    raise QueueError("unexpected binary data in %s: %r"%(name,
                                                                    data[name]))
                _file_write('%s/%s'%(temp,name), 0, self.umask, data[name])
            elif self.type[name] == 'string':
                if type(data[name]) not in [str, unicode]:
                    raise QueueError("unexpected string data in %s: %r"%(name,
                                                                    data[name]))
                _file_write('%s/%s'%(temp,name), 1, self.umask, data[name])
            elif self.type[name] == 'table':
                if not isinstance(data[name], dict):
                    raise QueueError("unexpected table data in %s: %r"%(name,
                                                                    data[name]))
                _file_write('%s/%s'%(temp,name), 1, self.umask,
                            _hash2string(data[name]))
            else:
                raise QueueError("unexpected data type in %s: %r"%(name,
                                                            self.type[name]))
        for name in self.mandatory.keys():
            if not data.has_key(name):
                raise QueueError("missing mandatory data: %s"%name)
        while True:
            name = '%s/%s' % (self._insertion_directory(), _new_name())
            path = '%s/%s' % (self.path, name)
            try:
                os.rename(temp, path)
                return name
            except StandardError, e:
                if e.errno != errno.ENOTEMPTY or e.errno != errno.EEXIST:
                    raise OSError("cannot rename(%s, %s): %s"%(temp, path,
                                                               str(e)))
                    # RACE: the target directory was already present...
    enqueue = add
    """Alias for add()
    """

    def purge(self, maxtemp=300, maxlock=600):
        """Purge the queue:
         - delete unused intermediate directories
         - delete too old temporary directories
         - unlock too old locked directories
        Arguments:
            maxtemp - maximum time for a temporary element
            maxlock - maximum time for a locked element
        Raise:
            OSError - problem deleting element from disk
        note:
         - this uses first()/next() to iterate so this will reset the cursor
        """
        # get the list of intermediate directories
        _list = []
        for name in _directory_contents(self.path):
            if _DirectoryRegexp.match(name):
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
        # get the list of temporary and obsolete directories
        _list = []
        for name in _directory_contents('%s/%s'%(self.path,TEMPORARY_DIRECTORY), True):
            if _ElementRegexp.match(name):
                _list.append('%s/%s' % (TEMPORARY_DIRECTORY, name))
        for name in _directory_contents('%s/%s'%(self.path,OBSOLETE_DIRECTORY), True):
            if _ElementRegexp.match(name):
                _list.append('%s/%s' % (OBSOLETE_DIRECTORY, name))
        # remove the ones which are too old
        oldtime = time.time() - maxtemp
        for name in _list:
            path = '%s/%s' % (self.path, name)
            if _older(path, oldtime):
                _warn("* removing too old volatile element: %s" % name)
                for file in _directory_contents(path, True):
                    if file == LOCKED_DIRECTORY:
                        continue
                    fpath = '%s/%s' % (path, file)
                    try:
                        os.unlink(fpath)
                    except StandardError, e:
                        if e.errno != errno.ENOENT:
                            raise OSError("cannot unlink(%s): %s"%(fpath,
                                                                   str(e)))
            _special_rmdir('%s/%s' % (path, LOCKED_DIRECTORY))
            _special_rmdir(path)
        # iterate to find abandoned locked entries
        oldtime = time.time() - maxlock
        name = self.first()
        while name:
            if self._state('%s/%s' % (self.path, name)) != STATE_LOCKED:
                name = self.next()
                continue
            if not _older('%s/%s'%(self.path,name), oldtime):
                name = self.next()
                continue
            # TODO: check if remove_element is needed or "unlocking" instead.
            _warn("* removing too old locked element: %s" % name)
            self.unlock(name, True)


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
                    if isinstance(_q, Queue):
                        if _q.path in [x.path for x in self.qset]:
                            raise QueueError("queue already in the set: %s"%\
                                              _q.path)
                        self.qset.append(_q.copy())
                    else:
                        raise TypeError("Queue objects expected.")
                break
            elif isinstance(q, Queue):
                type_queue = True
                self.qset.append(q.copy())
            else:
                raise TypeError("expected Queue object(s) or list/tuple of "+\
                                 "Queue objects")

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
        if not isinstance(queue, Queue):
            raise TypeError("Queue objects expected.")
        for i,q in enumerate(self.qset):
            if queue.path == q.path:
                del self.qset[i]
                if self.elts:
                    del self.elts[i]
