#!/usr/bin/env python
# encoding: utf8

"""Producer, browser and consumer.
"""

from dirq.queue import Queue, QueueError, QueueLockError
import os
import sys

# total number of elements
COUNT = 9
# queue head directory
path = '/tmp/dirq-test-%i' % os.getpid()
# max elements per elements directory
maxelts = 3
# element's schema
schema = {'body'  : 'string',
          'header': 'table?'}

# ========
# PRODUCER
print "*** PRODUCER"
dirq_p = Queue(path, maxelts=maxelts, schema=schema)

print "adding %d elements to the queue at %s" % (COUNT, path)
done = 1
while done <= COUNT:
    element = {}
    element['body'] = u'Élément %i \u263A\n' % done
    if done % 2: # header only for odd sequential elements
        element['header'] = dict(os.environ)
    name = dirq_p.enqueue(element)
    #name = dirq_p.add(element) # same
    print "added %.2i: %s" % (done, name)
    done += 1

print "counting total elements in the queue:",
total_stored = dirq_p.count()
print " %i" % total_stored
assert total_stored == COUNT
del dirq_p
print '='*25

# =======
# BROWSER
print "*** BROWSER: Python iterator protocol."
dirq_b = Queue(path, schema=schema)
miss = 0
# for i,name in enumerate(dirq_b): # same (object returns Python iterator over
                                   # the names of elements)
for i,name in enumerate(dirq_b.names()):
    i += 1 # enumerate(o, start=1) in Python 2.6
    print "element: %s %s" % (path, name)
    try:
        if i in [1,4,5]: # artificially lock some elements
            os.mkdir('%s/%s/locked' % (path,name))
        e = dirq_b.get_element(name) # lock(name), get(name), unlock(name)
    except QueueLockError, e:
        print e
        miss += 1
        continue
    except QueueError, e:
        print e
        miss += 1
        continue
    except Exception, e:
        print e
        break
    print "element: %i" % i, e.keys()
print "found %i elements; got %i" % (i,i-miss)
del dirq_b

print "*** BROWSER: lower level iterator protocol."
dirq_b = Queue(path, schema=schema)
done = 0
miss = 0
name = dirq_b.first()
while name:
    print "element: %s %s" % (path, name)
    if not dirq_b.lock(name):
        name = dirq_b.next()
        miss += 1
        continue
    e = dirq_b.get(name)
    print "element: %i" % done, e.keys()
    dirq_b.unlock(name)
    done += 1
    name = dirq_b.next()
print "found %i elements; got %i" % (done+miss, done)
assert done == (COUNT - miss)
del dirq_b
print '='*25

# ========
# CONSUMER
print "*** CONSUMER: Python iterator protocol."
dirq_c = Queue(path, schema=schema)
miss = 0
for i,name in enumerate(dirq_c):
    i += 1 # enumerate(o, start=1) in Python 2.6
    print "element %i: %s %s" % (i, path, name)
    try:
        e = dirq_c.dequeue(name) # lock(name), get(name), remove(name)
    except QueueError, e:
        print e
        miss += 1
        continue
    except Exception, e:
        print e
        break
    print e.keys()
print "consumed %i elements out of %i seen" % (i-miss, i)
print "counting elements left in the queue:",
total_left = dirq_c.count()
print " %i" % total_left
assert total_left == miss
del dirq_c

print "*** CONSUMER: lower level iterator protocol."
path = '/tmp/dirq-test-%i-0' % os.getpid()
print "create new queue and add elements"
print "adding %d elements to the queue at %s" % (COUNT, path)
dirq_p = Queue(path, maxelts=maxelts, schema=schema)
done = 1
while done <= COUNT:
    element = {}
    element['body'] = u'Élément %i \u263A\n' % done
    if done % 2: # header only for odd sequential elements
        element['header'] = dict(os.environ)
    name = dirq_p.enqueue(element)
    print "added %.2i: %s" % (done, name)
    done += 1

print "start consuming..."
dirq_c = Queue(path, schema=schema)
done = 0
name = dirq_c.first()
while name:
    print "element: %s %s" % (path, name)
    if not dirq_c.lock(name):
        print "couldn't lock: %s" % name
        name = dirq_c.next()
        continue
    e = dirq_c.get(name)
    print e.keys()
    dirq_c.remove(name)
    done += 1
    name = dirq_c.next()
print "consumed %i elements" % done
print "counting elements left in the queue:",
total_left = dirq_c.count()
print " %i" % total_left
assert total_left == 0
