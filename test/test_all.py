# -*- coding: utf-8 -*-
try:
    import redis
    import testDirQueueRedis
except ImportError:
    pass
import testDirQueueBase
import testDirQueue
import testDirQueueSimple
import testDirQueueNull
import testDirQueueSet
import test_dirq
import test_dirqset

def main():
    redis_test = False
    if "redis" in globals():
        red = redis.Redis()
        redis_test = True
        try:
            red.set("foo", "bar")
        except redis.exceptions.ConnectionError:
            redis_test = False
    print('=' * 25)
    print('Running unit tests.')
    print('=' * 25)
    testDirQueueBase.main()
    testDirQueue.main()
    testDirQueueSimple.main()
    if redis_test:
        testDirQueueRedis.main()
    else:
        print("*** Redis test: redis not found")
    testDirQueueNull.main()
    testDirQueueSet.main()
    print('=' * 25)
    print('Running functional tests.')
    print('=' * 25)
    print('*** Queue')
    test_dirq.main_simple()
    print('*** QueueSimple')
    test_dirq.main_simple(simple=True)
    if redis_test:
        print('*** QueueRedis')
        test_dirq.main_simple(redis=True)
    else:
        print("*** Redis test: redis not found")
    print('*** QueueSet')
    test_dirqset.main_complex()

if __name__ == "__main__":
    main()
