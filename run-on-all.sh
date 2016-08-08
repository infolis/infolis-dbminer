#!/bin/bash
DATASETS_ROOT=${DATASETS_ROOT:-$HOME/infolis-datasets/datasets}
declare -A dataset_names=(
    [replication-wiki]="$DATASETS_ROOT/replication-wiki/text/all"
    [ssoar]="$DATASETS_ROOT/ssoar/text/all"
    [mpra]="$DATASETS_ROOT/mpra/text/all"
    [econstor]="$DATASETS_ROOT/econstor/text/all"
    [econstor2000]="$DATASETS_ROOT/econstor/text/first2000"
)
declare -A filemeta=(
    [$DATASETS_ROOT/replication-wiki/text/all]="$DATASETS_ROOT/replication-wiki/meta"
    [$DATASETS_ROOT/ssoar/text/all]="$DATASETS_ROOT/ssoar/meta"
    [$DATASETS_ROOT/mpra/text/all]="$DATASETS_ROOT/mpra/meta"
    [$DATASETS_ROOT/econstor/text/all]="$DATASETS_ROOT/econstor/meta"
    [$DATASETS_ROOT/econstor/text/first2000]="$DATASETS_ROOT/econstor/meta"
)
declare -A patternsets=(
    [dara]="import/dara-solr.json"
    [db]="import/databases.json"
    [icpsr]="import/icpsr-studies.json"
)

timestamp() {
    date -Iseconds | sed 's/[^0-9]\+/-/g'
}

usage () {
    echo "$(basename $0) <patternset> [dataset...]

        Patternsets: ${!patternsets[*]}

        Datasets: ${!dataset_names[*]}

        DATASETS_ROOT: $DATASETS_ROOT
    "
}

main () {
    declare -a datasets
    patternset="$1"; shift;
    if [[ -z "$patternset" || -z "${patternsets[$patternset]}" ]];then
        echo "no such patternset '$patternset'"
        usage
        exit 1
    fi
    if [[ -z "$1" ]];then
        datasets=("${!dataset_names[@]}")
    else
        datasets=("$@")
    fi
    local fm
    for fm in "${datasets[@]}";do
        if [[ -z "${dataset_names[$fm]}" ]];then
            echo "No such dataset '${fm}'"
            usage
            exit 2
        fi
    done
    local dataset textdir metadir outfile
    for dataset in "${datasets[@]}";do
        textdir="${dataset_names[$dataset]}"
        metadir="${filemeta[$textdir]}"
        outfile="output-${dataset}-${patternset}-$(timestamp).json"
        ./dbminer.py search-patterns "${patternsets[$patternset]}" "$textdir" "$metadir" "$outfile"
    done
}

main "$@"
