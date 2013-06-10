#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test program for testing dirq.QueueSet module.
"""

import os
import re
import shutil
import sys
import tempfile
import time
from optparse import OptionParser

sys.path.insert(1, re.sub('/\w*$', '', os.getcwd()))
import dirq
from dirq import queue

OS = ''
TEST = ''


def init():
    """ Initialize. """
    global OS, TEST
    parser = OptionParser(usage="%prog [OPTIONS] [--] TEST",
                          version=("%%prog %s" % dirq.VERSION))
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
    parser.add_option("--maxelts", dest="maxelts", type='int', default=0,
                      help="set the maximum number of elements per directory")

    OS, args = parser.parse_args()
    if not OS.path:
        print("*** mandatory option not set: path")
        sys.exit(1)
    if len(args) != 0:
        TEST = args[0]
    else:
        parser.print_help()
        sys.exit()


def debug(format, *arguments):
    """Report a debugging message.
    """
    if not OS.debug:
        return
    message = format % arguments
    message = re.sub('\s+$', '.', message)
    sys.stderr.write("# %s %s[%d]: %s\n" %
                     (time.strftime("%Y/%m/%d-%H:%M:%S",
                                    time.localtime(time.time())),
                      os.path.basename(sys.argv[0]),
                      os.getpid(), message))


def new_dirq(path, _schema):
    """Create a new dirq.Queue object, optionally with schema.
    """
    if _schema:
        schema = {'body': 'string'}
    else:
        schema = {}
    return queue.Queue(path, maxelts=OS.maxelts, schema=schema)


def new_dirqs():
    """Create a new Directory::Queue object, optionally with schema.
    """
    time1 = time.time()
    qs = queue.QueueSet(OS.path.split(','))
    debug("created queue set in %.4f seconds", time.time() - time1)
    return qs


def test_count():
    """Count the elements in the queue.
    """
    qs = new_dirqs()
    time1 = time.time()
    count = qs.count()
    time2 = time.time()
    debug("queue set has %d elements", count)
    debug("done in %.4f seconds", time2 - time1)


def test_add():
    """Add elements to the queue.
    """


def test_complex():
    """Add elements to the queue.
    """
    wd = OS.path
    os.mkdir(wd)
    qn = 6
    paths = []
    for i in range(qn):
        paths.append(wd + '/q%i' % i)
    count = OS.count or 1000
    debug("creating %i initial queues. adding %i elements into each." %
          (qn, count))
    queues = []
    t1 = time.time()
    while qn:
        q = new_dirq(paths[qn - 1], 1)
        debug("adding %d elements to the queue...", count)
        element = {}
        done = 0
        time1 = time.time()
        while not count or done < count:
            done += 1
            element['body'] = 'Element %i \u263A\n' % done
            q.add(element)
        time2 = time.time()
        debug("done in %.4f seconds", time2 - time1)
        queues.append(q)
        qn -= 1
    debug("total done in %.4f seconds", time.time() - t1)

    time1 = time.time()
    i = 3
    qs = queue.QueueSet(queues[0:i])
    debug("created queue set in %.4f seconds", time.time() - time1)
    debug("elements in %i queues: %i" % (i, qs.count()))

    debug("adding remaining queues to the set.")
    t1 = time.time()
    qs.add(queues[i:])
    debug("done in %.4f sec." % (time.time() - t1))
    debug("total element with added queues: %i" % qs.count())

    debug("removing %i first queues." % i)
    t1 = time.time()
    for q in queues[0:i]:
        qs.remove(q)
    debug("done in %.4f sec." % (time.time() - t1))

    debug("number of elements left: %i" % qs.count())

    debug("deleting queues from disk...")
    for path in paths:
        shutil.rmtree(path, ignore_errors=True)
    debug("done.")


def test_iterate():
    """Iterate through the set of queues (only lock+unlock).
    """
    debug("iterating all elements in the set of queues (one pass)...")
    qs = new_dirqs()
    done = 0
    time1 = time.time()
    name = qs.first()
    while name:
        if not qs.lock(name):
            name = qs.next()
            continue
        qs.unlock(name)
        done += 1
        name = qs.next()
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)


def main_complex():
    """A wrapper to run from a library.
    """
    global OS

    class options(object):
        """ options class. """
        path = tempfile.mkdtemp() + '/dirqset'
        count = 100
        random = False
        size = False
        header = False
        debug = True
        maxelts = 0
    OS = options()
    try:
        test_complex()
    except Exception:
        error = sys.exc_info()[1]
        shutil.rmtree(OS.path, ignore_errors=True)
        raise error
    shutil.rmtree(OS.path, ignore_errors=True)

if __name__ == "__main__":
    init()
    if TEST == "count":
        test_count()
    elif TEST == "add":
        test_add()
    elif TEST == "complex":
        test_complex()
    elif TEST == "iterate":
        test_iterate()
    else:
        print("unsupported test:", TEST)
