
import errno
import re
import os
import time

from dirq.QueueBase import QueueBase, _name, _file_create, _special_mkdir,\
    _file_read, _DirectoryRegexp, _ElementRegexp, _special_rmdir, _warn

# suffix indicating a temporary element
TEMPORARY_SUFFIX = ".tmp"

# suffix indicating a locked element
LOCKED_SUFFIX = ".lck"

class QueueSimple(QueueBase):
    def __init__(self, path, umask=None, granularity=60):
        """
        path - queue top level directory
        umask - the umask to use when creating files and directories (default:
                use the running process' umask)
        granularity - the time granularity for intermediate directories
                      (default: 60)
        """
        super(QueueSimple, self).__init__(path, umask=umask)

        if not isinstance(granularity, int):
            raise TypeError('granularity should be integer.')
        self.granularity = granularity
        if self.granularity == 0:
            self._add_dir = self.__add_dir_timecurrent

    def _add_dir(self):
        t = time.time()
        t -= t % self.granularity
        return "%08x" % t

    def __add_dir_timecurrent(self):
        return "%08x" % time.time()

    def _add_data(self, data):
        """Write 'data' to a file.
        Return: (tuple) directory name where the file was written, full path to
        the temporary file.
        """
        _dir = self._add_dir()
        while 1:
            tmp = '%s/%s/%s%s' % (self.path, _dir, _name(), TEMPORARY_SUFFIX)
            try:
                fh = _file_create(tmp, umask=self.umask)
            except EnvironmentError, ex:
                if ex.errno == errno.ENOENT:
                    _special_mkdir('%s/%s/' % (self.path, _dir))
                    continue
            else:
                if fh:
                    break
        fh.write(data)
        fh.close()
        return _dir, tmp

    def _add_path(self, tmp, _dir):
        """Given temporary file and directory where it resides: create a hard
        link to that file and remove initial one.
        Return: element name (<directory name>/<file name>).
        """
        while 1:
            name = _name()
            new = '%s/%s/%s' % (self.path, _dir, name)
            try:
                os.link(tmp, new)
            except OSError, ex:
                if ex.errno != errno.EEXIST:
                    raise ex
                else:
                    continue
            os.unlink(tmp)
            return '%s/%s' % (_dir, name)

    def _build_elements(self):
        _list = []
        while self.dirs:
            _dir = self.dirs.pop(0)
            for name in os.listdir('%s/%s' % (self.path,_dir)):
                if _ElementRegexp.match(name):
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

    add_ref = add

    def add_path(self, path):
        """Add the given file (identified by its path) to the queue and return
        the corresponding element name, the file must be on the same
        filesystem and will be moved to the queue
        """
        _dir = self._add_dir()
        _special_mkdir('%s/%s' % (self.path, _dir), umask=self.umask)
        return self._add_path(path, _dir)

    def get(self, name):
        """Get locked element.
        """
        return _file_read('%s/%s%s' % (self.path, name, LOCKED_SUFFIX), False)

    get_ref = get
    "Defined to comply with Directory::Queue interface."

    def get_path(self, name):
        return '%s/%s%s' % (self.path, name, LOCKED_SUFFIX) 

    def lock(self, name, permissive=True):
        path = '%s/%s' % (self.path, name)
        lock = '%s%s' % (path, LOCKED_SUFFIX)
        try:
            os.link(path, lock)
        except OSError, ex:
            if permissive and (ex.errno == errno.EEXIST or 
                                    ex.errno == errno.ENOENT):
                return False
            e = OSError("cannot link(%s, %s): %s" % (path, lock, str(ex)))
            e.errno = ex.errno
            raise e
        else:
            t = time.time()
            os.utime(path, (t, t))
            return True

    def unlock(self, name, permissive=False):
        """
        """
        path = '%s/%s' % (self.path, name)
        lock = '%s%s' % (path, LOCKED_SUFFIX)
        try:
            os.unlink(lock)
        except OSError, ex:
            if permissive and ex.errno == errno.ENOENT:
                return 0
            raise ex
        else:
            return 1

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
            if _DirectoryRegexp.match(name):
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
            for el in os.listdir('%s/%s' % (self.path, name)):
                if _ElementRegexp.match(el):
                    count += 1
        return count

    def purge(self, maxtemp=300, maxlock=600):
        """Purge the queue by removing unused intermediate directories, removing
        too old temporary elements and unlocking too old locked elements (aka
        staled locks); note: this can take a long time on queues with many
        elements.

        maxtemp - maximum time for a temporary element (in seconds, default 300);
                  if set to 0, temporary elements will not be removed
        maxlock - maximum time for a locked element (in seconds, default 600);
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
                                        if re.search('(%s|%s)$'%(TEMPORARY_SUFFIX,LOCKED_SUFFIX), x)]
                for old in tmp_lock_elems:
                    stat = os.stat('%s/%s' % (path, old))
                    if old.endswith(TEMPORARY_SUFFIX) and stat.st_mtime >= oldtemp:
                        continue
                    if old.endswith(LOCKED_SUFFIX) and stat.st_mtime >= oldlock:
                        continue
                    _warn("WARNING: removing too old volatile file: %s/%s" % (
                                                                self.path, old))
                    os.unlink('%s/%s' % (path, old))
        # try to purge all but the last intermediate directory
        if len(dirs) > 1:
            dirs.sort()
            dirs.pop()
            for _dir in dirs:
                path = '%s/%s' % (self.path, _dir)
                if len(os.listdir(path)) == 0:
                    _special_rmdir(path)
