#!/bin/bash

last=$(git describe --tags --abbrev=0 2>/dev/null)

if [ "$last" = "" ]; then
  git log --pretty=format:'%s' | sort -k2n | uniq > ./releaseNotes.tmp
else
  git log --pretty=format:'%s' $last..HEAD | sort -k2n | uniq > ./releaseNotes.tmp
fi

function part() {
  name=$1
  pattern=$2
  changes=$(grep -P '\[\w+-\d+\]\s*<('$pattern')>:' ./releaseNotes.tmp | sed -E 's/ *<('$pattern')>//' |sed 's/[ci skip]\s*//' | awk -F: '{print "- " $1 ":" $2}'|sort|uniq)
  changes2=$(grep -P '^('$pattern')(\(.*\))?:' ./releaseNotes.tmp | sed -E 's/^('$pattern')(\(.*\)):\s*/**\2**: /' | sed -E 's/^('$pattern'):\s*//'|sed -E 's/\[ci skip\]\s*//' | awk '{print "- " $0}' |sort|uniq)
  lines1=$(printf "\\$changes" |wc -l)
  lines2=$(printf "\\$changes2" |wc -l)
  lines=$(expr $line1 + $lines2)
  #echo $name $pattern $lines >&2
  if [ $lines -gt 0 ]; then
    echo "### $name:"
    echo ""
    [ $lines1 -gt 0 ] && echo "$changes"
    [ $lines2 -gt 0 ] && echo "$changes2"
    echo ""
  fi
}

part "Features" "feature|feat"
part "Bug Fixes" "bugfix|fix"
part "Enhancements" "enhance|enh"
part "Tests" "test"
part "Documents" "docs|doc"

rm -f ./releaseNotes.tmp
