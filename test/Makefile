ifndef PYTHON
	PYTHON=python
endif

TESTDIR := $(shell [ "$(shell uname)" = "Darwin" ] && \
							mktemp -d -t dirq-test || mktemp -d -t dirq-test.XXXXXX)

QueueBase: testDirQueueBase.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
Queue: testDirQueue.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
QueueSet: testDirQueueSet.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?

QueueSimple: testDirQueueSimple.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?

QueueFunc: test_dirq.py
	@echo "=== $? Queue ==="
	@rm -rf $(TESTDIR)
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 10 -p $(TESTDIR) simple

QueueSimpleFunc: test_dirq.py
	@echo "=== $? QueueSimple ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 10 -p $(TESTDIR) --simple simple

QueueSetFunc: test_dirqset.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 10 -p $(TESTDIR) complex

tests: QueueBase Queue QueueSimple QueueSet QueueFunc QueueSimpleFunc QueueSetFunc

test: tests clean
 
clean:
	@echo "=== cleanup ==="
	rm -rf $(TESTDIR)
	rm -f *.pyc
