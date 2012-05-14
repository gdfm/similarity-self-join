#!/bin/bash

if [ "$1" == "map" ] ; then
	BEG="SUCCESS MAP"
	END="SUCCESS REDUCE"
elif [ "$1" == "red" ] ; then
	BEG="SUCCESS REDUCE"
	END="SUCCESS CLEANUP"
else
	echo "$0 <map|red>"
	exit 1
fi

sed -n '
/'"$BEG"'/,/'"$END"'/ {
	s/SUCCESS.*//
	s/task//
	p }
' | awk -F '[()]' '{print $2}' | grep -v "^$" | awk -F\, '{if ($1 ~ /hrs/) print $1*3600+$2*60+$3; else if ($1 ~ /mins/) print $1*60+$2 ; else print $1+0}' 
