import codecs
import errno
import os
import time
import sys

UPID = '%01x' % (os.getpid() % 16)

def _name():
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

def _special_getdir():
    raise NotImplementedError()

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
        data = fh.read()
    except StandardError, e:
        raise IOError("cannot read %s: %s"%(path, str(e)))
    try:
        fh.close()
    except StandardError, e:
        raise OSError("cannot close %s: %s"%(path, str(e)))
    return data

def _file_create(): 
    raise NotImplementedError()

def _file_write(path, utf8, umask, data):
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
        fh.write(data)
    except StandardError, e:
        raise IOError("cannot write to %s: %s"%(path, str(e)))
    try:
        fh.close()
    except StandardError, e:
        raise OSError("cannot close %s: %s"%(path, str(e)))

class QueueBase(object):
    def __init__(self, path, umask=None):
        """
        Arguments:
            path
                the queue toplevel directory
            umask
                the umask to use when creating files and directories
                (default: use the running process' umask)
        Raise:
            TypeError  - wrong input data types provided
            OSError    - can't create directory structure
        """
        self.dirs = []
        self.elts = []
        self._next_exception = False

        if not isinstance(path, (str, unicode)):
            raise TypeError("'path' should be str or unicode")
        self.path = path
        if umask != None or isinstance(umask, int):
            raise TypeError("'umask' should be integer")
        self.umask = umask

        # create top level directory
        path = ''
        for d in self.path.split('/'):
            path = '%s/%s' % (path, d)
            _special_mkdir(path, self.umask)

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

    def _reset(self):
        raise NotImplementedError

    def first(self):
        """Return the first element in the queue and cache information about
        the next ones.
        Raise:
            OSError - can't list directories
        """
        self._reset()
        return self.next()

    def _build_elements(self):
        raise NotImplementedError

#    next()
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

    def touch(self, ename):
        """Touch an element directory to indicate that it is still being used.
        note:
         - this is only really useful for locked elements but we allow it for all

        Raises:
         EnvironmentError - on any IOError, OSError in utime()

        TODO: this may not work on OSes with directories implemented not as
              files (eg. Windows). See doc for os.utime().
        """
        path = '%s/%s' % (self.path, ename)
        try:
            os.utime(path, None)
        except (IOError, OSError), e:
            raise EnvironmentError("cannot utime(%s, None): %s" % (path, str(e)))
