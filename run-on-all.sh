#!/bin/bash
set -ex
DATASETS=$HOME/infolis-datasets/datasets
declare -A filemeta=(
    [$DATASETS/replication-wiki/text/all]="$DATASETS/replication-wiki/meta"
    [$DATASETS/ssoar/text/all]="$DATASETS/ssoar/meta"
    [$DATASETS/mpra/text/all]="$DATASETS/mpra/meta"
)
declare -A patternsets=(
    [dara]="import/dara-solr.json"
    [db]="import/databases.json"
    [icpsr]="import/icpsr-studies.json"
)

timestamp() {
    date -Iseconds | sed 's/[^0-9]//g'
}

usage () {
    echo "$(basename $0) <patternset>

        Patternsets: ${!patternsets[*]}
    "
}
patternset="$1"

if [[ -z "${patternsets[$patternset]}" ]];then
    echo "no such patternset '$patternset'"
    usage
    exit 1
fi


for textdir in "${!filemeta[@]}";do
    metadir="${filemeta[$textdir]}"
    outfile="output-${patternset}-$(timestamp).json"
    ./dbminer.py search-patterns "${patternsets[$patternset]}" "$textdir" "$metadir" "$outfile"
done
