#!/bin/bash

# Copyright (c) 2021 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

ci=$(realpath $(dirname $0))
newv=$1
if [ "$newv" = "" ]; then
  echo "$0 <version>"
  exit 1
fi
echo "__version__ = '$newv'" > taos/_version.py
sed -E '3s#version.*#version = "'$newv'"#' pyproject.toml >> pyproject.toml2
mv pyproject.toml2 pyproject.toml

sed -n "1,9p" CHANGELOG.md > CHANGELOG.md2
printf "## v$newv - $(date +%F)\n\n" >> CHANGELOG.md2
$ci/changelog-generate.sh >> CHANGELOG.md2
sed "1,9d" CHANGELOG.md >> CHANGELOG.md2
mv CHANGELOG.md2 CHANGELOG.md

# git commit -a -m "release: v$newv"
# git push
# git tag v$newv
# git push --force origin v$newv:v$newv
# bash build_doc.sh
