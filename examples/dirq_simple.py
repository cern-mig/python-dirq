#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Producer, browser and consumer. """

from dirq.queue import Queue, QueueError, QueueLockError
import os
import sys
import tempfile

# total number of elements
COUNT = 9
# queue head directory
path = tempfile.mkdtemp()
# max elements per elements directory
maxelts = 3
# element's schema
schema = {'body': 'string',
          'header': 'table?'}

# ========
# PRODUCER
print("*** PRODUCER")
dirq_p = Queue(path, maxelts=maxelts, schema=schema)

print("adding %d elements to the queue at %s" % (COUNT, path))
done = 1
while done <= COUNT:
    element = {}
    try:
        element['body'] = ('Élément %i \u263A\n' % done).decode("utf-8")
    except AttributeError:
        element['body'] = 'Élément %i \u263A\n' % done
    if done % 2:  # header only for odd sequential elements
        element['header'] = dict(os.environ)
    name = dirq_p.enqueue(element)
    # name = dirq_p.add(element) # same
    print("added %.2i: %s" % (done, name))
    done += 1

total_stored = dirq_p.count()
print("total elements in the queue: %d" % total_stored)
assert total_stored == COUNT
del dirq_p
print('=' * 25)

# =======
# BROWSER
print("*** BROWSER: Python iterator protocol.")
dirq_b = Queue(path, schema=schema)
done = 0
miss = 0
for i, name in enumerate(dirq_b.names()):
    done += 1  # enumerate(o, start=1) in Python 2.6
    print("element: %s %s" % (path, name))
    try:
        if i in [1, 4, 5]:  # artificially lock some elements
            os.mkdir('%s/%s/locked' % (path, name))
        e = dirq_b.get_element(name)  # lock(name), get(name), unlock(name)
    except QueueLockError:
        error = sys.exc_info()[1]
        print(error)
        miss += 1
        continue
    except QueueError:
        error = sys.exc_info()[1]
        print(error)
        miss += 1
        continue
    except Exception:
        error = sys.exc_info()[1]
        print(error)
        break
    print("element: %i" % i, e.keys())
print("found %i elements; got %i" % (done, done - miss))
del dirq_b

print("*** BROWSER: lower level iterator protocol.")
dirq_b = Queue(path, schema=schema)
done = 0
miss = 0
name = dirq_b.first()
while name:
    print("element: %s %s" % (path, name))
    if not dirq_b.lock(name):
        name = dirq_b.next()
        miss += 1
        continue
    element = dirq_b.get(name)
    print("element: %i" % done, element.keys())
    dirq_b.unlock(name)
    done += 1
    name = dirq_b.next()
print("found %i elements; got %i" % (done + miss, done))
assert done == (COUNT - miss)
del dirq_b
print('=' * 25)

# ========
# CONSUMER
print("*** CONSUMER: Python iterator protocol.")
dirq_c = Queue(path, schema=schema)
miss = 0
for i, name in enumerate(dirq_c):
    i += 1  # enumerate(o, start=1) in Python 2.6
    print("element %i: %s %s" % (i, path, name))
    try:
        e = dirq_c.dequeue(name)  # lock(name), get(name), remove(name)
    except QueueError:
        error = sys.exc_info()[1]
        print(error)
        miss += 1
        continue
    except Exception:
        error = sys.exc_info()[1]
        print(error)
        break
    print(e.keys())
print("consumed %i elements out of %i seen" % (i - miss, i))
total_left = dirq_c.count()
print("elements left in the queue: %d" % total_left)
assert total_left == miss
del dirq_c

print("*** CONSUMER: lower level iterator protocol.")
path = tempfile.mkdtemp()
print("create new queue and add elements")
print("adding %d elements to the queue at %s" % (COUNT, path))
dirq_p = Queue(path, maxelts=maxelts, schema=schema)
done = 1
while done <= COUNT:
    element = {}
    try:
        element['body'] = ('Élément %i \u263A\n' % done).decode("utf-8")
    except AttributeError:
        element['body'] = 'Élément %i \u263A\n' % done
    if done % 2:  # header only for odd sequential elements
        element['header'] = dict(os.environ)
    name = dirq_p.enqueue(element)
    print("added %.2i: %s" % (done, name))
    done += 1

print("start consuming...")
dirq_c = Queue(path, schema=schema)
done = 0
name = dirq_c.first()
while name:
    print("element: %s %s" % (path, name))
    if not dirq_c.lock(name):
        print("couldn't lock: %s" % name)
        name = dirq_c.next()
        continue
    element = dirq_c.get(name)
    print(element.keys())
    dirq_c.remove(name)
    done += 1
    name = dirq_c.next()
print("consumed %i elements" % done)
total_left = dirq_c.count()
print("elements left in the queue: %d" % total_left)
assert total_left == 0
