#!/usr/bin/env python

import csv
import json
import sys
import re
import glob
import os.path
import xml.etree.ElementTree as ET

TSV_HEADER = {
    "ID": 0,
    "TITLE": 1,
    'URL': 3,
    "KEYWORDS": 2
}

def oai_to_entity():
    pass

def apply(dbfile, textdir, metadir, outdbfile):
    with open(dbfile) as jsoninfile:
        db = json.load(jsoninfile)
        for textfile in glob.glob(textdir + "/*.txt"):
            metafile = metadir + "/" + os.path.splitext(os.path.basename(textfile))[0] + ".xml"
            # TODO continue
            print(textfile, metafile)


def tsv_to_json(infile, outfile):
    db = { "entity": {}, "infolisPattern": {} }
    with open(infile) as csvfile:
        tsvreader = csv.reader(csvfile, delimiter='\t')
        for row in tsvreader:
            db['entity'][row[TSV_HEADER['ID']]] = {
                "identifier": row[TSV_HEADER['URL']],
                "name": row[TSV_HEADER['TITLE']]
            }
            for pat in row[TSV_HEADER['KEYWORDS']].split(" ; "):
                db['infolisPattern']["dbpat-" + re.sub("[^a-zA-Z0-9]", "-", pat)] = {
                    "regexPattern": pat,
                    "linkTo": row[TSV_HEADER['ID']]
                }
        with open(outfile, mode="w") as jsonfile:
            jsonfile.write(json.dumps(db, indent=2))

def print_usage(exit_code):
    prog = sys.argv[0]
    print("Usage: " + prog + " <command> [args...]")
    print("""
    Commands:

    tsv-to-json <tsv> <json>
        Convert TSV file <tsv> to JSON file <json>

    apply <db> <textdir> <metadir> <outdb>
        Run all the patterns from <db> on the files in <textdir>
        and create entities from the data <metadir> and link
        them to the pattern-generating entities and write to <outdb>
    """)
    sys.exit(exit_code)



if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print_usage(0)
    cmd = sys.argv[1]
    if cmd == 'tsv-to-json':
        if len(sys.argv) != 4:
            print_usage(1)
        tsv_to_json(sys.argv[2], sys.argv[3])
    elif cmd == 'apply':
        if len(sys.argv) != 6:
            print_usage(1)
        apply(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        print_usage(1)
