ifndef PYTHON
	PYTHON=python
endif

TESTDIR=/tmp/dirq-test.$$$$

QueueBase: testDirQueueBase.py
	@echo "--- $? ---"
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
Queue: testDirQueue.py
	@echo "--- $? ---"
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
QueueSet: testDirQueueSet.py
	@echo "--- $? ---"
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?

QueueSimple: testDirQueueSimple.py
	@echo "--- $? ---"
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?

QueueFunc: test_dirq.py
	@echo "--- $? ---"
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 10 -p $(TESTDIR) simple

QueueSetFunc: test_dirqset.py
	@echo "--- $? ---"
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 10 -p $(TESTDIR) complex

tests: QueueBase Queue QueueSimple QueueSet QueueFunc QueueSetFunc

test: tests clean
 
clean:
	@echo "--- cleanup ---"
	rm -rf $(TESTDIR)
	rm -f *.pyc

