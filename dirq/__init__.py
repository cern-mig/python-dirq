"""Directory based queue.

A port of Perl module Directory::Queue
http://search.cpan.org/~lcons/Directory-Queue-1.3/
The documentation from Directory::Queue module was adapted for Python.

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue.

For usage and implementation details see 'dirq.queue' module.

AUTHOR

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

LICENSE AND COPYRIGHT

ASL 2.0

Copyright (C) 2010-2011
"""

AUTHOR = "Konstantin Skaburskas <konstantin.skaburskas@gmail.com>"
VERSION = '1.0.1'
DATE = "04 December 2011"
__author__ = AUTHOR
__version__ = VERSION
__date__ = DATE

from .queue import Queue
 