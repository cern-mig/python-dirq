"""Base class and common code for :py:mod:`dirq` package.

It is used internally by :py:mod:`dirq` modules and should not 
be used elsewhere.

Author
------

Konstantin Skaburskas \<konstantin.skaburskas@gmail.com\>

License and Copyright
---------------------

ASL 2.0

Copyright (C) 2010-2012
"""
import dirq
__author__ = dirq.AUTHOR
__version__ = dirq.VERSION
__date__ = dirq.DATE

import re
import codecs
import errno
import os
import time
import sys
import inspect

UPID = '%01x' % (os.getpid() % 16)

__DirectoryRegexp = '[0-9a-f]{8}'
_DirectoryRegexp  = re.compile('(%s)$' % __DirectoryRegexp)
__ElementRegexp   = '[0-9a-f]{14}'
_ElementRegexp    = re.compile('(%s)$' % __ElementRegexp)
_DirElemRegexp    = re.compile('^%s/%s$'%(__DirectoryRegexp,
                                          __ElementRegexp))

WARN = False

def _warn(text):
    if WARN:
        sys.stdout.write('%s, at %s line %s\n' % (text, __name__,
                                        inspect.currentframe().f_back.f_lineno))

def _name():
    """
    Return the name of a new element to (try to) use with:
    * 8 hexadecimal digits for the number of seconds since the Epoch
    * 5 hexadecimal digits for the microseconds part
    * 1 hexadecimal digit from the pid to further reduce name collisions

    Properties:
    * fixed size (14 hexadecimal digits)
    * likely to be unique (with high-probability)
    * can be lexically sorted
    * ever increasing (for a given process)
    * reasonably compact
    * matching $_ElementRegexp
    """
    t = time.time()
    return "%08x%05x%s" % (t, (t % 1.0)*100000, UPID)

def _directory_contents(path, missingok=True):
    """Get the contents of a directory as a list of names, without . and ..
    
    Raise:
        OSError - can't list directory
        
    Note:
    * if the optional second argument is true, it is not an error if the
      directory does not exist (anymore)
    """
    try:
        return os.listdir(path)
    except StandardError, e:
        if not missingok and not e.errcode == errno.ENOENT:
            raise OSError("cannot listdir(%s): %s"%(path, str(e)))
            # RACE: this path does not exist (anymore)
        return []

def _special_mkdir(path, umask=None):
    """
    Recursively create directories specified in path:
    * return true on success
    * return false if something with the same path already exists
    * die in case of any other error

    Raise:
        OSError - can't make directory
    
    Note:
    * in case something with the same name already exists, we do not check
      that this is indeed a directory as this should always be the case here
    """
    try:
        if umask == None:
            os.makedirs(path)
        else:
            oldumask = os.umask(umask)
            os.makedirs(path)
            os.umask(oldumask)
    except OSError, e:
        if e.errno == errno.EEXIST and not os.path.isfile(path):
            return False
        elif e.errno == errno.EISDIR:
            return False
        raise OSError("cannot mkdir(%s): %s"%(path, str(e)))
    else:
        return True

def _special_rmdir(path):
    """
    Delete a directory:
    * return true on success
    * return false if the path does not exist (anymore)
    * die in case of any other error
     
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

def _file_create(path, umask=None, utf8=False):
    """Open a file defined by 'path' and return file handler.
    
    Raises:
        OSError - if file exists
    """
    if umask:
        oldumask = os.umask(umask)
    if utf8:
        if os.path.exists(path):
            ex = OSError("[Errno %i] File exists: %s" % (errno.EEXIST, path))
            ex.errno = errno.EEXIST
            raise ex
        fh = codecs.open(path, 'w', 'utf8')
    else:
        fh = os.fdopen(os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL), 'w')
    if umask:
        os.umask(oldumask)

    return fh

def _file_write(path, utf8, umask, data):
    """Write to a file.
    
    Raise:
        OSError - problems opening/closing file
        IOError - file write error
    """
    fh = _file_create(path, umask=umask, utf8=utf8)
    try:
        fh.write(data)
    except StandardError, e:
        raise IOError("cannot write to %s: %s"%(path, str(e)))
    try:
        fh.close()
    except StandardError, e:
        raise OSError("cannot close %s: %s"%(path, str(e)))

class QueueBase(object):
    """QueueBase
    """
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
        if umask != None and not isinstance(umask, int):
            raise TypeError("'umask' should be integer")
        self.umask = umask

        # create top level directory
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

        Note:

        * the main purpose is to copy/clone the iterator cached state
        * the other structured attributes (including schema) are not cloned
        """
        import copy
        c = copy.deepcopy(self)
        c.dirs = []
        c.elts = []
        return c

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

    def _build_elements(self):
        raise NotImplementedError('Implement in sub-class.')

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
        if self._build_elements():
            return self.elts.pop(0)
        if self._next_exception:
            self._next_exception = False
            raise StopIteration
        else:
            return ''

    def touch(self, ename):
        """Touch an element directory to indicate that it is still being used.
        
        Note:
            this is only really useful for locked elements but we allow it 
            for all.

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
