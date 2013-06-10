ifndef PYTHON
	PYTHON=python
endif

TESTDIR := $(shell [ "$(shell uname)" = "Darwin" ] && \
             mktemp -d -t dirq-test || mktemp -d -t dirq-test.XXXXXX)

QueueBase: testDirQueueBase.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
	@echo

Queue: testDirQueue.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
	@echo

QueueSimple: testDirQueueSimple.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
	@echo

QueueNull: testDirQueueNull.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
	@echo

QueueSet: testDirQueueSet.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
	@echo

QueueFunc: test_dirq.py
	@echo "=== $? Queue ==="
	@rm -rf $(TESTDIR)
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 1000 -p $(TESTDIR) simple
	@echo

QueueSimpleFunc: test_dirq.py
	@echo "=== $? QueueSimple ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 1000 -p $(TESTDIR) --type simple simple
	@echo

QueueSetFunc: test_dirqset.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $? -d -c 100 -p $(TESTDIR) complex
	@echo

tests: QueueBase Queue QueueSimple QueueSet QueueFunc QueueSimpleFunc QueueSetFunc

test: tests clean

clean:
	@echo "=== cleanup ==="
	rm -rf $(TESTDIR)
	rm -f *.pyc
