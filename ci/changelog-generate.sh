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

set -ex
path=$1
prefix=$2
last=
if [ "$prefix" = "" ]; then
  last=$(git describe --tags --abbrev=0 2>/dev/null)
else
  last=$(git tag --sort=-committerdate |grep $prefix |head -n1 2>/dev/null)
fi

if [ "$last" = "" ]; then
  git log --pretty=format:'%s' $path | sort -k2n | uniq > ./releaseNotes.tmp
else
  git log --pretty=format:'%s' $last..HEAD $path | sort -k2n | uniq > ./releaseNotes.tmp
fi

function part() {
  name=$1
  pattern=$2
  changes=$(egrep -E '\[\w+-\d+\]\s*<('$pattern')>:' ./releaseNotes.tmp | sed -E 's/ *<('$pattern')>//' |sed 's/[ci skip]\s*//' | awk -F: '{print "- " $1 ":" $2}'| sed -E 's/^\s+|\s$//' | sort|uniq)
  changes2=$(egrep -E '^('$pattern')(\(.*\))?:' ./releaseNotes.tmp | sed -E 's/^('$pattern')(\(.*\)):\s*/**\2**: /' | sed -E 's/^('$pattern'):\s*//'|sed -E 's/\[ci skip\]\s*//' | awk '{print "- " $0}' | sed -E 's/^\s+|\s$//' |sort|uniq)

  if [ "$changes" != "" ] || [ "$changes2" != "" ]; then
    echo "### $name:"
    echo ""
    [ "$changes" != "" ] && echo "$changes"
    [ "$changes2" != "" ] && echo "$changes2"
    echo ""
  fi
}

part "Features" "feature|feat|impr"
part "Bug Fixes" "bugfix|fix"
part "Enhancements" "enhance|enh"
part "Tests" "test"
part "Documents" "docs|doc"

rm -f ./releaseNotes.tmp
