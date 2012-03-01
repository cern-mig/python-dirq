#!/bin/sh -e

# Bundle source release.

RELEASE=${1:?"Provide tag number to bundle into source release."}

git checkout -q rel-$RELEASE
python setup.py sdist
git checkout master
