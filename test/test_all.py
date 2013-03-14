# -*- coding: utf-8 -*-
import testDirQueueBase
import testDirQueue
import testDirQueueSimple
import testDirQueueNull
import testDirQueueSet
import test_dirq
import test_dirqset


def main():
    print('=' * 25)
    print('Running unit tests.')
    print('=' * 25)
    testDirQueueBase.main()
    testDirQueue.main()
    testDirQueueSimple.main()
    testDirQueueNull.main()
    testDirQueueSet.main()
    print('=' * 25)
    print('Running functional tests.')
    print('=' * 25)
    print('*** Queue')
    test_dirq.main_simple()
    print('*** QueueSimple')
    test_dirq.main_simple(simple=True)
    print('*** QueueSet')
    test_dirqset.main_complex()

if __name__ == "__main__":
    main()
