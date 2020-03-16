#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test program for testing the dirq.queue and dirq.QueueSimple modules.
"""

import os
import random
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
TESTS = ['all', 'add', 'count', 'get', 'iterate', 'purge', 'remove', 'simple']
ProgramName = sys.argv[0]


def init():
    """ Initialize. """
    global opts, TEST
    parser = OptionParser(usage="%prog [OPTIONS] [--] TEST",
                          version="%%prog %s" % dirq.VERSION)
    parser.add_option("-l", "--list", dest="list", action="store_true",
                      default=False, help="list available tests")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                      default=False, help="show debugging information")
    parser.add_option("-p", "--path", dest="path", type="string", default="",
                      help="set the queue path")
    parser.add_option("-c", "--count", dest="count", type="int", default=0,
                      help="set the elements count")
    parser.add_option("-s", "--size", dest="size", type="int", default=0,
                      help="set the body size for added elements")
    parser.add_option("-r", "--random", dest="random", action="store_true",
                      default=False, help="randomize the body size")
    parser.add_option("--granularity", dest="granularity", type="int",
                      default=None, help="time granularity for intermediate "
                      "directories (QueueSimple)")
    parser.add_option("--header", dest="header", action="store_true",
                      default=False, help="set header for added elements")
    parser.add_option("--maxelts", dest="maxelts", type="int",
                      default=0, help="set the maximum number of elements per "
                      "directory")
    parser.add_option("--maxlock", dest="maxlock", type="int", default=None,
                      help="maximum time for a locked element. 0 - locked "
                      "elements will not be unlocked.")
    parser.add_option("--maxtemp", dest="maxtemp", type="int", default=None,
                      help="maximum time for a temporary element. "
                      "0 - temporary elements will not be removed.")
    parser.add_option("--rndhex", dest="rndhex", type="int",
                      default=None, help="set the queue random hex digit")
    parser.add_option("--sleep", dest="sleep", type="float", default=0,
                      help="sleep this amount of seconds before starting "
                           "the test(s)")
    parser.add_option("--type", dest="type", type="string", default="simple",
                      help="set the type of dirq (simple|normal)")
    opts, args = parser.parse_args()
    if opts.list:
        print("Tests: %s" % ', '.join(TESTS))
        sys.exit()
    if not opts.path:
        _die("%s: mandatory option not set: -p/--path", ProgramName)
    if len(args) != 0:
        TEST = args[0]
        if TEST not in TESTS:
            _die("Unsupported test '%s'.\nTEST should be one of: %s" %
                 (TEST, ', '.join(TESTS)))
    else:
        parser.print_help()
        sys.exit()


def _die(fmt, *arguments):
    """Report a fatal error."""
    sys.stderr.write(fmt % arguments + "\n")
    sys.stderr.flush()
    sys.exit(1)


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


def new_dirq(_schema):
    """Create a new Directory::Queue object, optionally with schema.
    """
    kwargs = {}
    if opts.type == "simple":
        if opts.granularity is not None:
            kwargs['granularity'] = opts.granularity
        if opts.rndhex is not None:
            kwargs['rndhex'] = opts.rndhex
        return QueueSimple(opts.path, **kwargs)
    else:
        if _schema:
            schema = {'body': 'string',
                      'header': 'table?'}
            kwargs['schema'] = schema
        if opts.maxelts:
            kwargs['maxelts'] = opts.maxelts
        if opts.rndhex is not None:
            kwargs['rndhex'] = opts.rndhex
        return queue.Queue(opts.path, **kwargs)


def test_count():
    """Count the elements in the queue.
    """
    dq = new_dirq(0)
    time1 = time.time()
    count = dq.count()
    time2 = time.time()
    debug("queue has %d elements", count)
    debug("done in %.4f seconds", time2 - time1)


def test_purge():
    """Purge the queue.
    """
    debug("purging the queue...")
    dq = new_dirq(0)
    time1 = time.time()
    pwkargs = {}
    if opts.maxtemp is not None:
        pwkargs['maxtemp'] = opts.maxtemp
    if opts.maxlock is not None:
        pwkargs['maxlock'] = opts.maxlock
    dq.purge(**pwkargs)
    time2 = time.time()
    debug("done in %.4f seconds", time2 - time1)


def _body(size, rand):
    """Return a test body"""
    if rand:
        # see Irwin-Hall in http://en.wikipedia.org/wiki/Normal_distribution
        rnd = 0.
        for _ in range(12):
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
    rnd = opts.random
    size = opts.size
    count = opts.count
    if count:
        debug("adding %d elements to the queue...", count)
    else:
        debug("adding elements to the queue forever...")
    dq = new_dirq(1)
    if opts.type == "simple":
        element = ''
    else:
        element = {}
        if opts.header:
            element['header'] = dict(os.environ)
    done = 0
    time1 = time.time()
    while not count or done < count:
        done += 1
        if size:
            if opts.type == "simple":
                element = _body(size, rnd)
            else:
                element['body'] = _body(size, rnd)
        else:
            if opts.type == "simple":
                element = 'Element %i ;-)\n' % done
            else:
                try:
                    element['body'] = ('Élément %d \u263A\n' %
                                       done).decode("utf-8")
                except AttributeError:
                    element['body'] = 'Élément %d \u263A\n' % done
        _ = dq.add(element)
    time2 = time.time()
    debug("done in %.4f seconds", time2 - time1)


def test_remove():
    """Remove elements from the queue.
    """
    count = opts.count
    if count:
        debug("removing %d elements from the queue...", count)
    else:
        debug("removing all elements from the queue (one pass)...")
    dq = new_dirq(0)
    done = 0
    if count:
        # loop to iterate until enough are removed
        time1 = time.time()
        while done < count:
            name = dq.first()
            while name and done < count:
                if not dq.lock(name):
                    name = dq.next()
                    continue
                dq.remove(name)
                done += 1
                name = dq.next()
        time2 = time.time()
        debug("done in %.4f seconds", time2 - time1)
    else:
        # one pass only
        time1 = time.time()
        name = dq.first()
        while name:
            if not dq.lock(name):
                name = dq.next()
                continue
            dq.remove(name)
            done += 1
            name = dq.next()
        time2 = time.time()
        debug("done in %.4f seconds (%d elements removed)",
              time2 - time1, done)


def test_iterate():
    """Iterate through the queue (only lock+unlock).
    """
    dq = new_dirq(0)
    done = 0
    debug("iterating all elements in the queue (first()/next())...")
    time1 = time.time()
    name = dq.first()
    while name:
        if not dq.lock(name):
            name = dq.next()
            continue
        dq.unlock(name)
        done += 1
        name = dq.next()
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)
    debug("iterating all elements in the queue (iterator protocol)...")
    time1 = time.time()
    for name in dq:
        if not dq.lock(name):
            continue
        dq.unlock(name)
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)


def test_get():
    """Get all elements from the queue.
    """
    debug("getting all elements in the queue (one pass)...")
    dq = new_dirq(1)
    done = 0
    time1 = time.time()
    name = dq.first()
    while name:
        if not dq.lock(name):
            name = dq.next()
            continue
        data = dq.get(name)
        if opts.type == "simple" and len(data) < 10:
            _die("unexpected element %s: %s" % (name, data))
        dq.unlock(name)
        done += 1
        name = dq.next()
    time2 = time.time()
    debug("done in %.4f seconds (%d elements)", time2 - time1, done)


def test_simple():
    """Simple test filling and emptying a brand new queue.
    """
    path = opts.path
    if os.path.exists(path):
        _die("%s: directory exists: %s", ProgramName, opts.path)
    if not opts.count:
        _die("%s: missing option: -count", ProgramName)
    time1 = time.time()
    test_add()
    test_count()
    test_get()
    test_remove()
    test_purge()
    time2 = time.time()

    def directory_contents(path):
        """Wrapeed os.listdir()"""
        try:
            return os.listdir(path)
        except OSError:
            _die("%s: couldn't listdir(%s)", ProgramName, path)
    subdirs = directory_contents(path)
    if opts.type == "simple":
        num_subdirs = 1
    else:
        num_subdirs = 3
    if len(subdirs) != num_subdirs:
        _die("%s: unexpected subdirs: %i", ProgramName, len(subdirs))
    shutil.rmtree(path, ignore_errors=True)
    debug("done in %.4f seconds", time2 - time1)


if __name__ == "__main__":
    init()
    if TEST == 'all':
        TESTS.remove('all')
        tests = TESTS
    else:
        tests = [TEST]

    if opts.sleep:
        time.sleep(opts.sleep)
    for test in tests:
        test_func = 'test_%s()' % test
        exec(test_func)
