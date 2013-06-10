#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test program for testing dirq.queue and dirq.QueueSimple modules.
"""

import os
import random
import re
import shutil
import sys
import tempfile
import time
from optparse import OptionParser

sys.path.insert(1, re.sub('/\w*$', '', os.getcwd()))
import dirq
from dirq import queue
from dirq.QueueSimple import QueueSimple

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
        _die("*** mandatory option not set: -p/--path")
        sys.exit(1)
    if len(args) != 0:
        TEST = args[0]
        if TEST not in TESTS:
            _die("Unsupported test '%s'.\nTEST should be one of: %s" %
                 (TEST, ', '.join(TESTS)))
    else:
        parser.print_help()
        sys.exit()


def _die(format, *arguments):
    sys.stderr.write(format % arguments + "\n")
    sys.stderr.flush()
    sys.exit(1)


def debug(format, *arguments):
    """Report a debugging message.
    """
    if not opts.debug:
        return
    message = format % arguments
    message = re.sub('\s+$', '.', message)
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
        return QueueSimple(opts.path, **kwargs)
    else:
        if _schema:
            schema = {'body': 'string',
                      'header': 'table?'}
            kwargs['schema'] = schema
        if opts.maxelts:
            kwargs['maxelts'] = opts.maxelts
        return queue.Queue(opts.path, **kwargs)


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
    pwkargs = {}
    if opts.maxtemp is not None:
        pwkargs['maxtemp'] = opts.maxtemp
    if opts.maxlock is not None:
        pwkargs['maxlock'] = opts.maxlock
    dirq.purge(**pwkargs)
    time2 = time.time()
    debug("done in %.4f seconds", time2 - time1)


def _body(size, rand):
    ''
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
    random = opts.random
    size = opts.size
    count = opts.count
    if count:
        debug("adding %d elements to the queue...", count)
    else:
        debug("adding elements to the queue forever...")
    dirq = new_dirq(1)
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
                element = _body(size, random)
            else:
                element['body'] = _body(size, random)
        else:
            if opts.type == "simple":
                element = 'Element %i ;-)\n' % done
            else:
                try:
                    element['body'] = ('Élément %d \u263A\n' %
                                       done).decode("utf-8")
                except AttributeError:
                    element['body'] = 'Élément %d \u263A\n' % done
        _ = dirq.add(element)
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
                name = dirq.next()
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
        debug("done in %.4f seconds (%d elements removed)",
              time2 - time1, done)


def test_iterate():
    """Iterate through the queue (only lock+unlock).
    """
    dirq = new_dirq(0)
    done = 0
    debug("iterating all elements in the queue (first()/next())...")
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
    debug("iterating all elements in the queue (iterator protocol)...")
    time1 = time.time()
    for name in dirq:
        if not dirq.lock(name):
            continue
        dirq.unlock(name)
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
        try:
            return os.listdir(path)
        except OSError:
            _die("%s: couldn't listdir(%s)", ProgramName, path)
            sys.exit(1)
    subdirs = directory_contents(path)
    if opts.type == "simple":
        num_subdirs = 1
    else:
        num_subdirs = 3
    if len(subdirs) != num_subdirs:
        _die("%s: unexpected subdirs: %i", ProgramName, len(subdirs))
    shutil.rmtree(path, ignore_errors=True)
    debug("done in %.4f seconds", time2 - time1)


def main_simple(type="simple"):
    """A wrapper to run from a library.
    """
    global opts

    class options(object):
        debug = True
        path = tempfile.mkdtemp() + '/dirq'
        count = 1000
        size = False
        random = False
        granularity = None
        header = False
        maxelts = 0
        maxtemp = None
        maxlock = None
        type = "simple"
    opts = options()
    opts.type = type
    try:
        shutil.rmtree(opts.path, ignore_errors=True)
        test_simple()
    except Exception:
        error = sys.exc_info()[1]
        shutil.rmtree(opts.path, ignore_errors=True)
        raise error
    shutil.rmtree(opts.path, ignore_errors=True)

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
