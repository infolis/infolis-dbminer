#!/usr/bin/env python

import io
import csv
import json
import sys
import re
import glob
import os.path
import xml.etree.ElementTree as ET
from hashlib import md5
import itertools
import Queue
import threading
import logging
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(threadName)-10s : %(message)s')
#  logging.basicConfig(level=logging.DEBUG)
logging.logThreads = 1

COUNTER = itertools.count()

TSV_HEADER = {
    "ID": 0,
    "TITLE": 1,
    'URL': 3,
    "KEYWORDS": 2
}

NS = {
    'dc': 'http://purl.org/dc/elements/1.1/',
    'oai': 'http://www.openarchives.org/OAI/2.0/'
}
MAX_THREADS = 5
FILES_PER_THREAD = 1000
ENTITY_CONFIDENCE = 0.97
ENTITY_RELATIONS = ['uses_database']
ENTITYT_LINKREASON = 'dbminer'

xmlnode_text = lambda (x) : x.text
urlescape = lambda (x) : re.sub("[^a-zA-Z0-9]", "-", x).lower()

def make_infolis_file_from_textfile(textfile, entity):
    infolis_file = {}
    infolis_file['_id'] = 'file_' + str(COUNTER.next())
    infolis_file['fileStatus'] = 'AVAILABLE'
    infolis_file['mediaType'] = 'text/plain'
    infolis_file['fileName'] = os.path.basename(textfile)
    with open(textfile, 'r') as textin:
        textcontents = textin.read()
        infolis_file['md5'] = md5(textcontents).hexdigest()
        textcontents = textcontents.decode('utf-8')
    infolis_file['manifestsEntity'] = entity['_id']
    return infolis_file, textcontents

def make_entity_from_oai(metafile):
    root = ET.parse(metafile)
    entity = {}
    entity['_id'] = 'entity_' + str(COUNTER.next())
    try:
        entity['identifier'] = root.find('.//dc:relation', NS).text
        entity['name'] = root.find(".//dc:title", NS).text
        entity['authors'] = map(xmlnode_text, root.findall('.//dc:creator', NS))
        entity['subjects'] = map(xmlnode_text, root.findall('.//dc:subject', NS))
        entity['language'] = root.find('.//dc:language', NS).text
        entity['abstractText'] = root.find('.//dc:description', NS).text
    except AttributeError, e:
        logging.warn("(%s): %s" % (metafile, e))
    return entity

def search_patterns_in_files(q, dbfile, textfiles, cur, total, metadir):
    with open(dbfile) as jsoninfile:
        db = json.load(jsoninfile)
        for textfile in textfiles[cur:total]:
            found = []
            metafile = metadir + "/" + os.path.splitext(os.path.basename(textfile))[0] + ".xml"
            entity = make_entity_from_oai(metafile)
            db['entity'][entity['_id']] = entity
            infolis_file, textcontents = make_infolis_file_from_textfile(textfile, entity)
            for infolisPattern in db['infolisPattern']:
                pat = db['infolisPattern'][infolisPattern]
                if pat['regexPattern'] in textcontents:
                    found.append(infolisPattern)
                    #  print(entity)
                    db['entityLink']['link_' + str(COUNTER.next())] = {
                            'confidence': ENTITY_CONFIDENCE,
                        'linkReason': ENTITYT_LINKREASON,
                        'entityRelations': ENTITY_RELATIONS,
                        'fromEntity': entity['_id'],
                        'toEntity': pat['linkTo']
                    }
            cur += 1
            if len(found) > 0:
                logging.info("%-5d %s"%(cur, found))
            else:
                logging.info("%-5d --"%(cur))
    q.put(db)

def search_patterns(dbfile, textdir, metadir, outdbfile):
    q = Queue.Queue()
    textfiles = glob.glob(textdir + "/*.txt")[0:2000]
    for x in range(0, len(textfiles), FILES_PER_THREAD):
        t = threading.Thread(target=search_patterns_in_files, args=(q, dbfile,
            textfiles, x, x+FILES_PER_THREAD, metadir))
        t.start()
        print(q)
        if threading.active_count() >= MAX_THREADS:
            q.wait()
    q.wait()
    print(q)
    #  while threading.active_count() < MAX_THREADS
    #  db = search_patterns_in_files(dbfile, textfiles, metadir)
    #  with open(outdbfile, 'w') as jsonoutfile:
        #  jsonoutfile.write(json.dumps(db, indent=2))

def tsv_to_json(infile, outfile):
    db = { "entity": {}, "infolisPattern": {}, 'entityLink': {} }
    with io.open(infile, 'r', encoding='utf8') as csvfile:
        tsvreader = csv.reader(csvfile, delimiter='\t')
        for row in tsvreader:
            db['entity'][row[TSV_HEADER['ID']]] = {
                "url": row[TSV_HEADER['URL']],
                "identifier": row[TSV_HEADER['URL']],
                "name": row[TSV_HEADER['TITLE']]
            }
            db['infolisPattern']['dbpat-' + urlescape(row[TSV_HEADER['TITLE']])] = {
                'regexPattern': row[TSV_HEADER['TITLE']],
                'linkTo': row[TSV_HEADER['ID']]
            }
            for pat in row[TSV_HEADER['KEYWORDS']].split(" ; "):
                if pat == '':
                    continue
                db['infolisPattern']["dbpat-" + urlescape(pat)] = {
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

    search-patterns <db> <textdir> <metadir> <outdb>
        Run all the patterns from <db> on the files in <textdir>
        and create entities from the data <metadir> and link
        them to the pattern-generating entities and write to <outdb>
    """)
    sys.exit(exit_code)



if __name__ == "__main__":
    #  make_entity_from_oai("./meta/oai_10001.xml")
    if len(sys.argv) < 2 or sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print_usage(0)
    cmd = sys.argv[1]
    if cmd == 'tsv-to-json':
        if len(sys.argv) != 4:
            print_usage(1)
        tsv_to_json(sys.argv[2], sys.argv[3])
    elif cmd == 'search-patterns':
        if len(sys.argv) != 6:
            print_usage(1)
        search_patterns(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        print_usage(1)
