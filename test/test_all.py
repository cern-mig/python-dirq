
import testDirQueue
import testDirQueueSet
import test_dirq
import test_dirqset

def main():
    print '='*25
    print 'Running unit tests.'
    print '='*25
    testDirQueue.main()
    testDirQueueSet.main()
    print '='*25
    print 'Running functional tests.'
    print '='*25
    print '*** Queue'
    test_dirq.main_simple()
    print '*** QueueSet'
    test_dirqset.main_complex()

if __name__ == "__main__":
    main()
