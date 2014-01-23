ifndef PYTHON
	PYTHON=python
endif

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

Wrapper: testWrapper.py
	@echo "=== $? ==="
	@export PYTHONPATH=`pwd`/..:${PYTHONPATH}; \
	$(PYTHON) $?
	@echo

tests: QueueBase Queue QueueSimple QueueNull QueueSet Wrapper

test: tests clean

clean:
	@echo "=== cleanup ==="
	rm -rf $(TESTDIR)
	rm -f *.pyc
