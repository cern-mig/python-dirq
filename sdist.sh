#!/bin/sh -e

RELEASE=${1:?}

git checkout -q rel-$RELEASE
python setup.py sdist
git checkout master
