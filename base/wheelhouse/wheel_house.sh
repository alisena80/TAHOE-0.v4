#!/bin/bash
mkdir wheelhouse cache
cd cache
# Downloads all dependencies and switches python version
(deactivate && pyenv activate 3.7 && pyenv exec pip install --upgrade pip setuptools wheel && pyenv exec pip3 wheel -r ../3.7requirements.txt --wheel-dir ../wheelhouse)
(deactivate && pyenv activate 3.6 && pyenv exec pip install --upgrade pip setuptools wheel && pyenv exec pip3 wheel -r ../3.6requirements.txt --wheel-dir ../wheelhouse)

# Creates index file for pip based lookup
cd ../wheelhouse
INDEXFILE="<html><head><title>Links</title></head><body><h1>Links</h1>"
for f in *.whl; do INDEXFILE+="<a href='$f'>$f</a><br>"; done
INDEXFILE+="</body></html>"
echo "$INDEXFILE" > index.html
cd ..


