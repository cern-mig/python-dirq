"""
Utilities used in dirq module.

Author
------

Konstantin Skaburskas <konstantin.skaburskas@gmail.com>

License and Copyright
---------------------

ASL 2.0

Copyright (C) CERN 2011-2021
"""

import sys

_PY2 = sys.hexversion < 0x03000000
_PY3 = not _PY2

if _PY2:
    VALID_STR_TYPES = (str, unicode)
else:
    VALID_STR_TYPES = (str, bytes)

if _PY2:
    VALID_INT_TYPES = (int, long)
else:
    VALID_INT_TYPES = (int, )


def is_bytes(string):
    """ Check if given string is a byte string. """
    if _PY2:
        return not isinstance(string, unicode)
    else:  # python 3
        return isinstance(string, bytes)
