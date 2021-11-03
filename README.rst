===========
python-dirq
===========

.. image:: https://github.com/cern-mig/python-dirq/actions/workflows/test.yml/badge.svg


Overview
========

The goal of this module is to offer a queue system using the underlying
filesystem for storage, security and to prevent race conditions via atomic
operations. It focuses on simplicity, robustness and scalability.

This module allows multiple concurrent readers and writers to interact with
the same queue. A Perl implementation of the same algorithm is available
at http://search.cpan.org/dist/Directory-Queue/ and a Java implementation at
https://github.com/cern-mig/java-dirq so readers and writers can be
written in different programming languages.

Install
=======

To install this module, run the following commands::

    python setup.py test
    python setup.py install

Usage
=====

For documentation and public interface::

    pydoc dirq.queue

or visit online documentation:

    http://dirq.readthedocs.org/

Examples
========

    examples/README

License and Copyright
=====================

Apache License, Version 2.0

Copyright (C) CERN 2011-2021
