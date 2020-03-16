#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test program for testing the dirq.QueueSet module.
"""

import os
import re
import shutil
import sys
import time

from optparse import OptionParser

sys.path.insert(1, re.sub(r'/\w*$', '', os.getcwd()))

import dirq  # noqa E402
from dirq import queue  # noqa E402
from dirq.QueueSimple import QueueSimple  # noqa E402

opts = None
TEST = ''
ProgramName = sys.argv[0]


def init():
    """ Initialize. """
    global opts, TEST
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
    parser.add_option("--granularity", dest="granularity", type="int",
                      default=None, help="time granularity for intermediate "
                      "directories (QueueSimple)")
    parser.add_option("--header", dest="header", action="store_true",
                      default=False, help="set header for added elements")
    parser.add_option("--maxelts", dest="maxelts", type='int', default=0,
                      help="set the maximum number of elements per directory")
    parser.add_option("--type", dest="type", type="string", default="simple",
                      help="set the type of dirq (simple|normal)")
    opts, args = parser.parse_args()
    if not opts.path:
        _die("%s: mandatory option not set: -p/--path", ProgramName)
    if len(args) != 0:
        TEST = args[0]
    else:
        parser.print_help()
        sys.exit()


def debug(fmt, *arguments):
    """Report a debugging message.
    """
    if not opts.debug:
        return
    message = fmt % arguments
    message = re.sub(r'\s+$', '.', message)
    sys.stderr.write("# %s %s[%d]: %s\n" %
                     (time.strftime("%Y/%m/%d-%H:%M:%S",
                                    time.localtime(time.time())),
                      os.path.basename(sys.argv[0]),
                      os.getpid(), message))


def _die(fmt, *arguments):
    """Report a fatal error."""
    sys.stderr.write(fmt % arguments + "\n")
    sys.stderr.flush()
    sys.exit(1)


def new_dirq(path, _schema):
    """Create a new Directory::Queue object, optionally with schema.
    """
    kwargs = {}
    if opts.type == "simple":
        if opts.granularity is not None:
            kwargs['granularity'] = opts.granularity
        return QueueSimple(path, **kwargs)
    else:
        if _schema:
            schema = {'body': 'string',
                      'header': 'table?'}
            kwargs['schema'] = schema
        if opts.maxelts:
            kwargs['maxelts'] = opts.maxelts
        return queue.Queue(path, **kwargs)


def new_dirqs():
    """Create a new Directory::Queue object, optionally with schema.
    """
    time1 = time.time()
    qs = queue.QueueSet()
    for path in opts.path.split(','):
        qs.add(new_dirq(path, 0))
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
    wd = opts.path
    os.mkdir(wd)
    qn = 6
    paths = []
    for i in range(qn):
        paths.append(wd + '/q%i' % i)
    count = opts.count or 1000
    debug("creating %i initial queues. adding %i elements into each." %
          (qn, count))
    queues = []
    t1 = time.time()
    while qn:
        dq = new_dirq(paths[qn - 1], 1)
        debug("adding %d elements to the queue...", count)
        element = {}
        done = 0
        time1 = time.time()
        while not count or done < count:
            done += 1
            element['body'] = 'Element %i \u263A\n' % done
            dq.add(element)
        time2 = time.time()
        debug("done in %.4f seconds", time2 - time1)
        queues.append(dq)
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
    for dq in queues[0:i]:
        qs.remove(dq)
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
    dq, name = qs.first()
    while dq:
        if not dq.lock(name):
            dq, name = qs.next()
            continue
        dq.unlock(name)
        done += 1
        dq, name = qs.next()
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)


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
        _die("%s: unsupported test: %s", ProgramName, TEST)
