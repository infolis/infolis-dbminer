#!/bin/sh


if [[ "$#" -lt 2 ]];then
    echo "Usage: $(basename "$0") <patternfile> <inputdir>"
    echo "   <patternfile> should be a list of patterns, one per line"
    echo "   <inputdir> should be a directory of *.txt files"
    exit 1;
fi

patternfile="$1"
inputdir="$2"

find "$inputdir" -type f -name '*.txt' -exec grep \
    -H \
    --only-matching \
    --max-count=1 \
    --with-filename \
    --word-regexp \
    --file="$patternfile" \
    "{}" \;
