"""
QueueSimple - object oriented interface to a simple directory based queue.

A port of Perl module Directory::Queue::Simple
http://search.cpan.org/dist/Directory-Queue/
The documentation from Directory::Queue::Simple module was
adapted for Python.

=================
QueueSimple class
=================

:py:class:`QueueSimple` - simple directory based queue.

Usage::

    from dirq.QueueSimple import QueueSimple

    # sample producer

    dirq = QueueSimple('/tmp/test')
    for count in range(1,101):
        name = dirq.add("element %i\\n" % count)
        print("# added element %i as %s" % (count, name))

    # sample consumer

    dirq = QueueSimple('/tmp/test')
    for name in dirq:
        if not dirq.lock(name):
            continue
        print("# reading element %s" % name)
        data = dirq.get(name)
        # one could use dirq.unlock(name) to only browse the queue...
        dirq.remove(name)


Description
-----------

    This module is very similar to dirq.queue, but uses a
    different way to store data in the filesystem, using less
    directories. Its API is almost identical.

    Compared to :py:mod:`dirq.queue`, this module:

    * is simpler
    * is faster
    * uses less space on disk
    * can be given existing files to store
    * does not support schemas
    * can only store and retrieve byte strings
    * is not compatible (at filesystem level) with Queue

    Please refer to :py:mod:`dirq.queue` for general information about
    directory queues.

Directory Structure
-------------------

    The toplevel directory contains intermediate directories that contain
    the stored elements, each of them in a file.

    The names of the intermediate directories are time based: the element
    insertion time is used to create a 8-digits long hexadecimal number.
    The granularity (see the constructor) is used to limit the number of
    new directories. For instance, with a granularity of 60 (the default),
    new directories will be created at most once per minute.

    Since there is usually a filesystem limit in the number of directories
    a directory can hold, there is a trade-off to be made. If you want to
    support many added elements per second, you should use a low
    granularity to keep small directories. However, in this case, you will
    create many directories and this will limit the total number of
    elements you can store.

    The elements themselves are stored in files (one per element) with a
    14-digits long hexadecimal name SSSSSSSSMMMMMR where:

    * SSSSSSSS represents the number of seconds since the Epoch

    * MMMMM represents the microsecond part of the time since the Epoch

    * R is a random digit used to reduce name collisions


    A temporary element (being added to the queue) will have a *.tmp*
    suffix.

    A locked element will have a hard link with the same name and the
    *.lck* suffix.


Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""

import errno
import os
import re
import sys
import time

from dirq.QueueBase import QueueBase, _name, _file_create, _special_mkdir, \
    _file_read, _DIRECTORY_REGEXP, _ELEMENT_REGEXP, _special_rmdir, _warn
from dirq.utils import is_bytes

# suffix indicating a temporary element
TEMPORARY_SUFFIX = ".tmp"

# suffix indicating a locked element
LOCKED_SUFFIX = ".lck"


class QueueSimple(QueueBase):
    """
    QueueSimple
    """
    def __init__(self, path, umask=None, rndhex=None, granularity=60):
        """
        * path - queue top level directory
        * umask - the umask to use when creating files and directories
                  (default: use the running process' umask)
        * rndhex - the hexadecimal digit to use in names
                  (default: randomly chosen)
        * granularity - the time granularity for intermediate directories
                      (default: 60)
        """
        super(QueueSimple, self).__init__(path, umask=umask, rndhex=rndhex)

        if not isinstance(granularity, int):
            raise TypeError('granularity should be integer.')
        self.granularity = granularity

    def _add_dir(self):
        """ Return new directory name based on time and granularity. """
        now = int(time.time())
        if self.granularity > 1:
            now -= now % self.granularity
        return "%08x" % now

    def _add_data(self, data):
        """Write 'data' to a file.

        Return: (tuple) directory name where the file was written, full path to
        the temporary file.
        """
        _dir = self._add_dir()
        while 1:
            tmp = '%s/%s/%s%s' % (self.path, _dir, _name(self.rndhex),
                                  TEMPORARY_SUFFIX)
            try:
                if is_bytes(data):
                    new_file = _file_create(tmp, umask=self.umask, utf8=False)
                else:
                    new_file = _file_create(tmp, umask=self.umask, utf8=True)
            except EnvironmentError:
                error = sys.exc_info()[1]
                if error.errno == errno.ENOENT:
                    _special_mkdir('%s/%s' % (self.path, _dir), self.umask)
                    continue
            else:
                if new_file:
                    break
        new_file.write(data)
        new_file.close()
        return _dir, tmp

    def _add_path(self, tmp, _dir):
        """Given temporary file and directory where it resides: create a hard
        link to that file and remove initial one.

        Return: element name (<directory name>/<file name>).
        """
        while 1:
            name = _name(self.rndhex)
            new = '%s/%s/%s' % (self.path, _dir, name)
            try:
                os.link(tmp, new)
            except OSError:
                error = sys.exc_info()[1]
                if error.errno != errno.EEXIST:
                    raise error
                else:
                    continue
            os.unlink(tmp)
            return '%s/%s' % (_dir, name)

    def _build_elements(self):
        _list = []
        while self.dirs:
            _dir = self.dirs.pop(0)
            for name in os.listdir('%s/%s' % (self.path, _dir)):
                if _ELEMENT_REGEXP.match(name):
                    _list.append(name)
            if not _list:
                continue
            _list.sort()
            self.elts = ['%s/%s' % (_dir, x) for x in _list]
            return True
        return False

    def add(self, data):
        """Add data to the queue as a file.

        Return: element name (<directory name>/<file name>).
        """
        _dir, path = self._add_data(data)
        return self._add_path(path, _dir)

    add_ref = add  # to comply with the Perl Directory::Queue interface

    def add_path(self, path):
        """Add the given file (identified by its path) to the queue and return
        the corresponding element name, the file must be on the same
        filesystem and will be moved to the queue
        """
        _dir = self._add_dir()
        _special_mkdir('%s/%s' % (self.path, _dir), self.umask)
        return self._add_path(path, _dir)

    def get(self, name):
        """Get locked element.
        """
        return _file_read('%s/%s%s' % (self.path, name, LOCKED_SUFFIX), False)

    get_ref = get  # to comply with the Perl Directory::Queue interface

    def get_path(self, name):
        """ Return the path given the name. """
        return '%s/%s%s' % (self.path, name, LOCKED_SUFFIX)

    def lock(self, name, permissive=True):
        """Lock an element.

        Arguments:
            name - name of an element
            permissive - work in permissive mode

        Return:

        * true on success
        * false in case the element could not be locked (in permissive
          mode)
        """
        path = '%s/%s' % (self.path, name)
        lock = '%s%s' % (path, LOCKED_SUFFIX)
        try:
            os.link(path, lock)
        except OSError:
            error = sys.exc_info()[1]
            if permissive and (error.errno == errno.EEXIST or
                               error.errno == errno.ENOENT):
                return False
            new_error = OSError("cannot link(%s, %s): %s" %
                                (path, lock, error))
            new_error.errno = error.errno
            raise new_error
        else:
            try:
                os.utime(path, None)
            except OSError:
                # RACE: the element file does not exist anymore
                # (this can happen if an other process locked & removed the
                # element while our link() was in progress...
                # yes, this can happen!)
                error = sys.exc_info()[1]
                if permissive and error.errno == errno.ENOENT:
                    os.unlink(lock)
                    return False
                new_error = OSError("cannot utime(%s, %s): %s" %
                                    (path, lock, error))
                new_error.errno = error.errno
                raise new_error
            else:
                return True

    def unlock(self, name, permissive=False):
        """Unlock an element.

        Arguments:
            name - name of an element
            permissive - work in permissive mode

        Return:

        * true on success
        * false in case the element could not be unlocked (in permissive
          mode)
        """
        lock = '%s/%s%s' % (self.path, name, LOCKED_SUFFIX)
        try:
            os.unlink(lock)
        except OSError:
            error = sys.exc_info()[1]
            if permissive and error.errno == errno.ENOENT:
                return False
            raise error
        else:
            return True

    def remove(self, name):
        """Remove a locked element from the queue.
        """
        path = '%s/%s' % (self.path, name)
        lock = '%s%s' % (path, LOCKED_SUFFIX)
        os.unlink(path)
        os.unlink(lock)

    def __get_list_of_interm_dirs(self, dirs):
        """Fill out provided list with names of intermediate directories.
        """
        for name in os.listdir(self.path):
            if _DIRECTORY_REGEXP.match(name):
                dirs.append(name)

    def count(self):
        """Return the number of elements in the queue, locked or not
        (but not temporary).
        """
        count = 0
        # get list of intermediate directories
        dirs = []
        self.__get_list_of_interm_dirs(dirs)
        # count elements in sub-directories
        for name in dirs:
            for element in os.listdir('%s/%s' % (self.path, name)):
                if _ELEMENT_REGEXP.match(element):
                    count += 1
        return count

    def purge(self, maxtemp=300, maxlock=600):
        """Purge the queue by removing unused intermediate directories,
        removing too old temporary elements and unlocking too old locked
        elements (aka staled locks); note: this can take a long time on
        queues with many elements.

        maxtemp - maximum time for a temporary element
                  (in seconds, default 300);
                  if set to 0, temporary elements will not be removed
        maxlock - maximum time for a locked element
                  (in seconds, default 600);
                  if set to 0, locked elements will not be unlocked
        """
        # get list of intermediate directories
        dirs = []
        self.__get_list_of_interm_dirs(dirs)
        # remove all but old temporary or locked elements
        oldtemp = maxtemp != 0 and time.time() - maxtemp or 0
        oldlock = maxlock != 0 and time.time() - maxlock or 0
        if oldtemp or oldlock:
            for _dir in dirs:
                path = '%s/%s' % (self.path, _dir)
                tmp_lock_elems = [x for x in os.listdir(path)
                                  if re.search('(%s|%s)$' %
                                               (TEMPORARY_SUFFIX,
                                                LOCKED_SUFFIX), x)]
                for old in tmp_lock_elems:
                    try:
                        stat = os.stat('%s/%s' % (path, old))
                    except OSError:
                        error = sys.exc_info()[1]
                        if error.errno == errno.ENOENT:
                            continue
                        raise error
                    if (old.endswith(TEMPORARY_SUFFIX) and
                            stat.st_mtime >= oldtemp):
                        continue
                    if old.endswith(LOCKED_SUFFIX) and \
                            stat.st_mtime >= oldlock:
                        continue
                    _warn("WARNING: removing too old volatile file: %s/%s" %
                          (self.path, old))
                    try:
                        os.unlink('%s/%s' % (path, old))
                    except OSError:
                        error = sys.exc_info()[1]
                        if error.errno != errno.ENOENT:
                            raise error
        # try to purge all but the last intermediate directory
        if len(dirs) > 1:
            dirs.sort()
            dirs.pop()
            for _dir in dirs:
                path = '%s/%s' % (self.path, _dir)
                if len(os.listdir(path)) == 0:
                    _special_rmdir(path)
