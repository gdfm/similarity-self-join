#!/bin/bash
[ "$#" -ge 2 ] || ( echo "Usage: $(basename $0) <bundle> <doc>" && exit 1 )
bundle=$1
doc=$2
echo "<DOC>" 
zcat input/"$bundle".gz | sed -n '/'$bundle'-'$doc'<\/DOCNO>/,/<\/DOC>/ p'
