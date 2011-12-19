"""Exceptions used in the module.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) 2010-2011
"""

class QueueError(Exception):
    """QueueError"""

class QueueLockError(QueueError):
    """QueueLockError"""
