#!/bin/bash
# pip3 install pdoc3
source .env
cd ..
pdoc --html --force --output-dir docs taosrest
pdoc --html --force --output-dir docs taos
sshpass -p "$DOC_HOST_PWD"  scp -r ./docs/* root@$DOC_HOST:/data/apidocs/taospy/