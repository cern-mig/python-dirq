"""Directory based queue.

A port of Perl module Directory::Queue
http://search.cpan.org/dist/Directory-Queue/

The documentation from Directory::Queue module was adapted for Python.

The goal of this module is to offer a queue system using the underlying
filesystem for storage, security and to prevent race conditions via atomic
operations. It focuses on simplicity, robustness and scalability.

This module allows multiple concurrent readers and writers to interact with
the same queue.

For usage and implementation details see :py:mod:`dirq.queue` module.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2013
"""

AUTHOR = "Konstantin Skaburskas <konstantin.skaburskas@gmail.com>"
VERSION = '1.4'
DATE = "14 Jun 2013"
__author__ = AUTHOR
__version__ = VERSION
__date__ = DATE

from dirq.queue import Queue
