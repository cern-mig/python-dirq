#!/usr/bin/env python
# encoding: utf-8

import os
import re
import sys
import time
import random
import shutil
from optparse import OptionParser

sys.path.insert(1, re.sub('/\w*$','/src',os.getcwd()))
from dirq import queue

OS = ''
TEST = ''
ProgramName = sys.argv[0]

def init():
    global OS, TEST
    parser = OptionParser(usage="%prog [OPTIONS] [--] TEST", version="%prog 0.1")
    parser.add_option('-d', '--debug', dest='debug', action="store_true",
                       default=False, help="show debugging information")
    parser.add_option('-p', '--path', dest='path', type='string', default='',
                      help="set the queue path")
    parser.add_option('-c', '--count', dest='count', type='int', default=0,
                      help="set the elements count")
    parser.add_option("-s", "--size", dest="size", type='int', default=0,
                      help="set the body size for added elements")
    parser.add_option("-r", "--random", dest="random", action="store_true",
                      default=False, help="randomize the body size")
    parser.add_option("--header", dest="header", action="store_true",
                      default=False, help="set header for added elements")
    parser.add_option("--maxelts", dest="maxelts", type='int',
                      default=0, help="set the maximum number of elements per directory")

    OS,args = parser.parse_args()
    if not OS.path:
        print "*** mandatory option not set: path"
        sys.exit(1)
    if len(args) != 0:
        TEST = args[0]
    else:
        parser.print_help()
        sys.exit()

def die(format, *arguments):
    print >> sys.stderr, format % arguments
    sys.exit(1)

def debug(format, *arguments):
    """Report a debugging message.
    """
    if not OS.debug:
        return
    message = format % arguments
    message = re.sub('\s+$', '.', message)
    sys.stderr.write("# %i [%5d] %s\n"%(time.time(),os.getpid(),message))

def new_dirq(_schema):
    """Create a new Directory::Queue object, optionally with schema.
    """
    if _schema:
        schema = {'body'  : 'string',
                  'header': 'table?'}
    else:
        schema = {}
    return queue.Queue(OS.path, maxelts=OS.maxelts, schema=schema)

def test_count():
    """Count the elements in the queue.
    """
    dirq = new_dirq(0)
    time1 = time.time()
    count = dirq.count()
    time2 = time.time()
    debug("queue has %d elements", count)
    debug("done in %.4f seconds", time2 - time1)

def test_purge():
    """Purge the queue.
    """
    debug("purging the queue...")
    dirq = new_dirq(0)
    time1 = time.time()
    dirq.purge(maxtemp=10, maxlock=10)
    time2 = time.time()
    debug("done in %.4f seconds", time2 - time1)

def _body(size, rand):
    ''
    if rand:
        # see Irwin-Hall in http://en.wikipedia.org/wiki/Normal_distribution
        rnd = 0.
        for i in range(12):
            rnd += random.random()
        rnd -= 6.
        rnd *= size / 6
        size += int(rnd)
    if size < 1:
        return ''
    return "A" * (size - 1) + "\n"

def test_add():
    """Add elements to the queue.
    """
    random = OS.random
    size = OS.size
    count = OS.count
    if count:
        debug("adding %d elements to the queue...", count)
    else:
        debug("adding elements to the queue forever...")
    dirq = new_dirq(1)
    element = {}
    if OS.header:
        element['header'] = dict(os.environ)
    done = 0
    time1 = time.time()
    while not count or done < count:
        done += 1
        if size:
            element['body'] = _body(size, random)
        else:
            element['body'] = u'Élément %i \u263A\n' % done
        name = dirq.add(element)
    time2 = time.time()
    debug("done in %.4f seconds", time2 - time1)

def test_remove():
    """Remove elements from the queue.
    """
    count = OS.count
    if count:
        debug("removing %d elements from the queue...", count)
    else:
        debug("removing all elements from the queue (one pass)...")
    dirq = new_dirq(0)
    done = 0
    if count:
        # loop to iterate until enough are removed
        time1 = time.time()
        while done < count:
            name = dirq.first()
            while name and done < count:
                if not dirq.lock(name):
                    name = dirq.next()
                    continue
                dirq.remove(name)
                done += 1
        time2 = time.time()
        debug("done in %.4f seconds", time2 - time1)
    else:
        # one pass only
        time1 = time.time()
        name = dirq.first()
        while name:
            if not dirq.lock(name):
                name = dirq.next()
                continue
            dirq.remove(name)
            done += 1
            name = dirq.next()
        time2 = time.time()
        debug("done in %.4f seconds (%d elements removed)", time2 - time1, done)

def test_iterate():
    """Iterate through the queue (only lock+unlock).
    """
    debug("iterating all elements in the queue (one pass)...")
    dirq = new_dirq(0)
    done = 0
    time1 = time.time()
    name = dirq.first()
    while name:
        if not dirq.lock(name):
            name = dirq.next()
            continue
        dirq.unlock(name)
        done += 1
        name = dirq.next()
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)

def test_get():
    """Get all elements from the queue.
    """
    debug("getting all elements in the queue (one pass)...")
    dirq = new_dirq(1)
    done = 0
    time1 = time.time()
    name = dirq.first()
    while name:
        if not dirq.lock(name):
            name = dirq.next()
            continue
        dirq.get(name)
        dirq.unlock(name)
        done += 1
        name = dirq.next()
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)

def test_simple():
    """Simple test filling and emptying a brand new queue.
    """
    path = OS.path
    if os.path.exists(path):
        die("%s: directory exists: %s", ProgramName, path)
    if not OS.count:
        die("%s: missing option: -count", ProgramName)
    time1 = time.time()
    test_add()
    test_count()
    test_get()
    test_remove()
    test_purge()
    time2 = time.time()
    def directory_contents(path):
        try:
            return os.listdir(path)
        except OSError, e:
            die("%s: couldn't listdir(%s)", ProgramName, path)
            sys.exit(1)
    subdirs = directory_contents(path)
    if len(subdirs) != 3:
        die("%s: unexpected subdirs: %i", ProgramName, len(subdirs))
    shutil.rmtree(path, ignore_errors=True)
    debug("done in %.4f seconds", time2 - time1)

def main_simple():
    """A wrapper to run from a library.
    """
    global OS
    class options(object):
        path = '/tmp/dirq-%i'%os.getpid()
        count = 10
        random = False
        size = False
        header = False
        debug = True
        maxelts = 0
    OS = options()
    try:
        shutil.rmtree(OS.path, ignore_errors=True)
        test_simple()
    except Exception, e:
        shutil.rmtree(OS.path, ignore_errors=True)
        raise e
    shutil.rmtree(OS.path, ignore_errors=True)

if __name__ == "__main__":
    init()
    if TEST == "count":
        test_count()
    elif TEST == "purge":
        test_purge()
    elif TEST == "add":
        test_add()
    elif TEST == "remove":
        test_remove()
    elif TEST == "iterate":
        test_iterate()
    elif TEST == "get":
        test_get()
    elif TEST == "simple":
        test_simple()
    else:
        print "unsupported test:", TEST
