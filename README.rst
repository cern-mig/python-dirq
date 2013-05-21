===========
python-dirq
===========

.. image:: https://secure.travis-ci.org/cern-mig/python-dirq.png?branch=master

Overview
========

Directory based queue.

A port of Perl module Directory::Queue

    http://search.cpan.org/dist/Directory-Queue/

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue.

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

Copyright (C) CERN 2011-2013
