
import dirq

NAME = 'dirq'
VERSION = dirq.VERSION
DESCRIPTION = "Directory based queue"
LONG_DESCRIPTION = """
A port of Perl module Directory::Queue
http://search.cpan.org/dist/Directory-Queue/

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue."""
AUTHOR = 'Konstantin Skaburskas'
AUTHOR_EMAIL = 'konstantin.skaburskas@gmail.com'
LICENSE = "ASL 2.0"
PLATFORMS = "Any"
URL = "http://code.google.com/p/dirq/"
DOWNLOAD_URL = "http://dirq.googlecode.com/files/dirq-%s.tar.gz" % VERSION
# Cheese shop (PyPI)
CLASSIFIERS = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: Unix",
    "Operating System :: Microsoft :: Windows",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.4",
    "Programming Language :: Python :: 2.5",
    "Programming Language :: Python :: 2.6",
    "Programming Language :: Python :: 3.0",
    "Topic :: Software Development :: Libraries :: Python Modules"
]

from distutils.core import setup, Command
import sys

class test(Command):
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        sys.path.insert(0, 'test/')
        import test_all
        test_all.main()

setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      platforms=PLATFORMS,
      url=URL,
      download_url=DOWNLOAD_URL,
      classifiers=CLASSIFIERS,
      packages=['dirq'],
      cmdclass={'test': test},
     )
