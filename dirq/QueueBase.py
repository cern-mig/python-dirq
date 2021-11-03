"""Base class and common code for :py:mod:`dirq` package.

It is used internally by :py:mod:`dirq` modules and should not
be used elsewhere.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""

import dirq
import codecs
import errno
import inspect
import os
import random
import re
import sys
import time

from dirq.utils import VALID_STR_TYPES

__author__ = dirq.AUTHOR
__version__ = dirq.VERSION
__date__ = dirq.DATE

__DIRECTORY_REGEXP = '[0-9a-f]{8}'
__ELEMENT_REGEXP = '[0-9a-f]{14}'
_DIRECTORY_REGEXP = re.compile('^(%s)$' % __DIRECTORY_REGEXP)
_ELEMENT_REGEXP = re.compile('^(%s)$' % __ELEMENT_REGEXP)
_DIRELT_REGEXP = re.compile('^%s/%s$' % (__DIRECTORY_REGEXP, __ELEMENT_REGEXP))

WARN = False


def _warn(text):
    """ Print a warning. """
    if WARN:
        sys.stdout.write('%s, at %s line %s\n' %
                         (text, __name__,
                          inspect.currentframe().f_back.f_lineno))
        sys.stdout.flush()


def _name(rndhex):
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
    * matching _ELEMENT_REGEXP
    """
    now = time.time()
    secs = int(now)
    msecs = int((now - secs) * 1000000)
    return "%08x%05x%01x" % (secs, msecs, rndhex)


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
    except Exception:
        error = sys.exc_info()[1]
        if not missingok and not error.errcode == errno.ENOENT:
            raise OSError("cannot listdir(%s): %s" % (path, error))
            # RACE: this path does not exist (anymore)
        return []


def _wrapped_makedirs(path):
    """Wrapped os.makedirs() used by _special_mkdir()"""
    try:
        os.makedirs(path)
    except OSError:
        error = sys.exc_info()[1]
        if error.errno == errno.EEXIST and os.path.isdir(path):
            return (False, None)
        elif error.errno == errno.EISDIR:
            return (False, None)
        return (False, "cannot mkdir(%s): %s" % (path, error))
    else:
        return (True, None)


def _special_mkdir(path, umask=None):
    """
    Recursively create directories specified in path:
    * return true on success
    * return false if the directory already exists
    * die in case of any other error

    Raise:
        OSError - can't make directory
    """
    if umask is None:
        result, error = _wrapped_makedirs(path)
    else:
        oldumask = os.umask(umask)
        result, error = _wrapped_makedirs(path)
        os.umask(oldumask)
    if error is None:
        return result
    raise OSError(error)


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
    except Exception:
        error = sys.exc_info()[1]
        if not error.errno == errno.ENOENT:
            raise OSError("cannot rmdir(%s): %s" % (path, error))
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
            fileh = codecs.open(path, 'r', "utf8")
        else:
            fileh = open(path, 'rb')
    except Exception:
        error = sys.exc_info()[1]
        raise OSError("cannot open %s: %s" % (path, error))
    try:
        data = fileh.read()
    except Exception:
        error = sys.exc_info()[1]
        raise IOError("cannot read %s: %s" % (path, error))
    try:
        fileh.close()
    except Exception:
        error = sys.exc_info()[1]
        raise OSError("cannot close %s: %s" % (path, error))
    return data


def _file_create(path, umask=None, utf8=False):
    """Open a file defined by 'path' and return file handler.

    Raises:
        OSError - if file exists
    """
    if umask is not None:
        oldumask = os.umask(umask)
    if utf8:
        if os.path.exists(path):
            ex = OSError("[Errno %i] File exists: %s" % (errno.EEXIST, path))
            ex.errno = errno.EEXIST
            raise ex
        fileh = codecs.open(path, 'w', 'utf8')
    else:
        fileh = os.fdopen(
            os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 438), 'wb')
    if umask is not None:
        os.umask(oldumask)

    return fileh


def _file_write(path, utf8, umask, data):
    """Write to a file.

    Raise:
        OSError - problems opening/closing file
        IOError - file write error
    """
    fileh = _file_create(path, umask=umask, utf8=utf8)
    try:
        fileh.write(data)
    except Exception:
        error = sys.exc_info()[1]
        raise IOError("cannot write to %s: %s" % (path, error))
    try:
        fileh.close()
    except Exception:
        error = sys.exc_info()[1]
        raise OSError("cannot close %s: %s" % (path, error))


class QueueBase(object):
    """QueueBase
    """
    def __init__(self, path, umask=None, rndhex=None):
        """
        Arguments:
            path
                the queue toplevel directory
            umask
                the umask to use when creating files and directories
                (default: use the running process' umask)
            rndhex
                the hexadecimal digit to use in names
                (default: randomly chosen)

        Raise:
            TypeError  - wrong input data types provided
            OSError    - can't create directory structure
        """
        self.dirs = []
        self.elts = []
        self._next_exception = False

        if type(path) not in VALID_STR_TYPES:
            raise TypeError("'path' should be str or unicode")
        self.path = path
        if umask is not None and not isinstance(umask, int):
            raise TypeError("'umask' should be integer")
        self.umask = umask
        if rndhex is not None and not isinstance(rndhex, int):
            raise TypeError("'rndhex' should be integer")
        if rndhex is None:
            self.rndhex = random.randint(0, 15)
        else:
            self.rndhex = rndhex % 16

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
        new = copy.deepcopy(self)
        new.dirs = []
        new.elts = []
        return new

    def _reset(self):
        """Regenerate list of intermediate directories. Drop cached
        elements list.

        Raise:
            OSError - can't list directories
        """
        self.dirs = []
        for name in _directory_contents(self.path):
            if _DIRECTORY_REGEXP.match(name):
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
        """ This should be implemented by sub classes. """
        raise NotImplementedError('Implement in sub-class.')

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
        if self._build_elements():
            return self.elts.pop(0)
        if self._next_exception:
            self._next_exception = False
            raise StopIteration
        else:
            return ''
    next = __next__

    def touch(self, ename):
        """Touch an element directory to indicate that it is still being used.

        Note:
            this is only really useful for locked elements but we allow it
            for all.

        Raises:
           EnvironmentError - on any IOError, OSError in utime()

        NOTE: this may not work on OSes with directories implemented not as
        files (eg. Windows). See doc for os.utime().
        """
        path = '%s/%s' % (self.path, ename)
        try:
            os.utime(path, None)
        except (IOError, OSError):
            error = sys.exc_info()[1]
            raise EnvironmentError("cannot utime(%s, None): %s" %
                                   (path, error))
