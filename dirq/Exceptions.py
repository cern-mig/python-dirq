"""Exceptions used in the module.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""


class QueueError(Exception):
    """QueueError"""


class QueueLockError(QueueError):
    """QueueLockError"""
