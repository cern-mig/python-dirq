#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Browse a set of queues. """

from dirq.queue import Queue, QueueSet
import tempfile

# root directory for queues
working_dir = tempfile.mkdtemp()
# number of queues
QUEUES = 4

print("*** Setup & populate queues")
# generate paths
paths = []
for i in range(QUEUES):
    paths.append(working_dir + '/q-%i' % i)
COUNT = 5

print("creating %i initial queues. adding %i elements into each." % (QUEUES,
                                                                     COUNT))
queues = []
queue_num = 0
while queue_num < QUEUES:
    queue = Queue(paths[queue_num], maxelts=5, schema={'body': 'string'})
    print("adding %d elements to the queue %s" % (COUNT, paths[queue_num]))
    element = {}
    done = 0
    while not COUNT or done < COUNT:
        done += 1
        element['body'] = 'Queue %i. Element %i' % (queue_num, done)
        queue.add(element)
    queues.append(queue)
    queue_num += 1
print("done.")


print("*** Browse")
i = 2
queue_set = QueueSet(queues[0:i])
queue_set_count = queue_set.count()
print("elements in %i queues: %i" % (i, queue_set_count))
assert queue_set_count == i * COUNT

print("adding remaining queues to the set.")
queue_set.add(queues[i:])
total_inset = queue_set.count()
print("total element with added queues: %d" % total_inset)
assert total_inset == QUEUES * COUNT

print("removing %i first queues." % i)
for q in queues[0:i]:
    queue_set.remove(q)

total_inset = queue_set.count()
print("number of elements left in the set: %d" % total_inset)
assert total_inset == (QUEUES - i) * COUNT

print("iterating over the elements left in the queue set")
for q, name in queue_set:
    print(q.path, name)
    print(q.get_element(name)['body'])
print("done.")
