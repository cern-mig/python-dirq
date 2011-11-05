import codecs
import errno
import os
import time

UPID = '%01x' % (os.getpid() % 16)

#_fatal()

#_name()
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

#_special_mkdir()
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

#_special_rmdir()
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

#_special_getdir()
def _special_getdir():
    raise NotImplementedError()

#_file_read()
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

#_file_create()
def _file_create(): 
    raise NotImplementedError()

#_file_write()
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

