#!/usr/bin/env python

"""Browse a set of queues.
"""

from dirq.queue import Queue, QueueSet
import os

# root directory for queues
wd = '/tmp'
pid = os.getpid()
# number of queues
QUEUES = 4

print "*** Setup & populate queues"
# generate paths
paths = []
for i in range(QUEUES):
    paths.append(wd+'/test-add-%i-%i'%(i, pid))
COUNT = 5

print "creating %i initial queues. adding %i elements into each." % (QUEUES,
                                                                     COUNT)
queues = []
qn = 0
while qn < QUEUES:
    q = Queue(paths[qn], maxelts=5, schema={'body' : 'string'})
    print "adding %d elements to the queue %s" % (COUNT, paths[qn])
    element = {}
    done = 0
    while not COUNT or done < COUNT:
        done += 1
        element['body'] = 'Queue %i. Element %i' % (qn, done)
        q.add(element)
    queues.append(q)
    qn += 1
print "done."


print "*** Browse"
i = 2
qs = QueueSet(queues[0:i])
print "elements in %i queues: %i" % (i, qs.count())

print "adding remaining queues to the set."
qs.add(queues[i:])
print "total element with added queues:",
total_inset = qs.count()
print " %i" % total_inset
assert total_inset == QUEUES*COUNT

print "removing %i first queues." % i
for q in queues[0:i]:
    qs.remove(q)

print "number of elements left in the set:",
total_inset = qs.count()
print " %i" % total_inset
assert total_inset == QUEUES*COUNT/2

print "iterating over the elements left in the queue set"
for q,name in qs:
    print q.path,name
    print q.get_element(name)['body']
print "done."