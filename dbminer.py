#!/usr/bin/env python

from hashlib import md5
import csv
import glob
import io
import itertools
import json
import logging
import os.path
import re
import resource
import subprocess
import sys
import time
try:
    import lxml.etree as ET
except ImportError:
    try:
        import xml.etree.cElementTree as ET
    except ImportError:
        import xml.etree.ElementTree as ET

#-----------------------------------------------------------------------------
# Configuration and Globals
# {{{

logging.basicConfig(level=logging.DEBUG,
    format='[%(levelname)s] %(asctime)s.%(msecs)03d - %(message)s',
    datefmt='%H:%M:%S')

COUNTER = itertools.count()

# save intermediate results every n files
BAK_INTERVAL = 500

DATABASES_CSV_HEADER = { "ID": 0, "TITLE": 1, "KEYWORDS": 2, 'URL': 3 }

ICPSRSTUDIES_CSV_HEADER = {
    "STUDY_NUMBER": 0,
    "OWNER": 1,
    "TITLE": 2,
    "INVESTIGATORS": 3,
    "DOI": 4,
    "TIME_PERIOD": 5 }

NS = {
    'dc': 'http://purl.org/dc/elements/1.1/',
    'oai': 'http://www.openarchives.org/OAI/2.0/'
}
ENTITY_CONFIDENCE = 0.97
ENTITY_RELATIONS = ['uses_database']
ENTITYT_LINKREASON = 'dbminer'

CLEAR = "\r"
for x in range(0,100):
    CLEAR+=' '
CLEAR += '\r'

xmlnode_text = lambda (x) : x.text
urlescape = lambda (x) : re.sub("[^a-zA-Z0-9]", "-", x).lower()

RE_CACHE = {}
def cachedRegex(r):
    if r not in RE_CACHE:
        RE_CACHE[r] = re.compile(r)
    return RE_CACHE[r]

def print_progress(cur, total, found, t0):
    """
    Show informative progress report on STDERR
    """
    if cur <= 1 or cur % 10 == 0:
        try:
            print_progress.throughput = cur / (time.time() - t0)
        except AttributeErrror, e:
            print_progress.throughput = 1
        try:
            seconds = (total - cur) / print_progress.throughput
            m, s = divmod(seconds, 60)
            h, m = divmod(m, 60)
            print_progress.eta = "%d:%02d:%02d" % (h, m, s)
        except AttributeErrror, e:
            print_progress.eta = "---"
    sys.stderr.write("\r[%-2.2f%%] %-5d/%5d [found: %4d] [throughput: %4.3f/s] [ETA: %s]"%(
        (cur * 1.0/total*100),
        cur,
        total,
        found,
        print_progress.throughput,
        print_progress.eta,
        ))

#
# }}}
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Generating objects
#{{{

def make_infolis_file_from_textfile(textfile, entity):
    """
    Create an InfolisFile from a text file and the entity it manifests
    """
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
    """
    Parse an entity from an OAI-PMH Dublin Core XML file
    """
    root = ET.parse(metafile)
    entity = {}
    try:
        entity['_id'] = 'entity_' + urlescape(root.find('.//dc:relation', NS).text)
        entity['identifier'] = root.find('.//dc:relation', NS).text
    except AttributeError, e:
        entity['_id'] = 'entity_' + urlescape(root.find('.//dc:identifier', NS).text)
        entity['identifier'] = root.find('.//dc:identifier', NS).text
    entity['name'] = root.find(".//dc:title", NS).text
    entity['authors'] = map(xmlnode_text, root.findall('.//dc:creator', NS))
    entity['subjects'] = map(xmlnode_text, root.findall('.//dc:subject', NS))
    entity['language'] = root.find('.//dc:language', NS).text
    try:
        entity['abstractText'] = root.find('.//dc:description', NS).text
    except AttributeError, e:
        logging.warn("Has no abstractText: %s"%e)

    return entity

def make_entity_from_solr_doc(db, elem):
    """
    Create an entity from a <doc> element in dara's solr response
    """
    entity = {}
    _id = 'daradoc_' + elem.find(".//str[@name='id']").text
    entity['_id'] = _id
    entity['identifier'] = "http://www.da-ra.de/dara/search/search_show?res_id=%s&lang=en&detail=true"%(elem.find(".//str[@name='id']").text)
    entity['name'] = elem.findtext(".//arr[@name='title']/str")
    if 'Rezension' in entity['name'] \
        or entity['name'] == 'Data' \
        or re.match(cachedRegex("^Figure.*"), entity['name']) \
        or re.match(cachedRegex("^Table.*"), entity['name']):
        #  logging.warn("Skip this, not a dataset: %s" % entity['name'])
        return
    entity['subjects'] = map(xmlnode_text, elem.findall(".//arr[@name='subject']/str"))
    entity['authors'] = map(xmlnode_text, elem.findall(".//arr[@name='person']/str"))
    try:
        entity['language'] = elem.findtext(".//arr[@name='studyLanguage_txt']/str")
        entity['_doi'] = elem.findtext(".//arr[@name='doi']/str")
    except AttributeError, e:
        logging.warn("has no studyLanguage_txt: %s", entity['identifier'])
        entity['language'] = 'eng'
    return entity

def make_entity_link_from_pattern(db, from_id, pat_id):
    """
    Create a link from an entity to another entity because of pattern
    """
    ret = {}
    infolisPattern = db['infolisPattern'][pat_id]
    conf = 1 / len(infolisPattern['linkTo'])
    for to_id in infolisPattern['linkTo']:
        db['entityLink']['link_' + str(COUNTER.next())] = {
            'confidence': conf,
            'linkReason': infolisPattern['regexPattern'],
            'entityRelations': ['matches_pattern'],
            'fromEntity': from_id,
            'toEntity': to_id
        }

def make_pattern(db, prefix, title, _id):
    """
    Adds to the 'infolisPattern' section of `db` a new entry or augments an
    existing entry for prefix with patterns for the title whose confidences
    are inverse to the number of matching patterns.

    Links to '_id'
    """
    #  logging.debug("Link %s/%s to %s" % (prefix, title, _id))
    regexify = lambda (x) : "\\b" + re.escape(x) + "\\b"
    created = 1
    # --------------------------
    # 1) Whole title
    # --------------------------
    db['infolisPattern'][prefix + '-' + urlescape(title)] = {
        'regexPattern': regexify(title),
        '_stringMatch': title,
        'linkTo': [_id]
    }
    # --------------------------
    # 2) Up to first comma, if result contains a space
    # --------------------------
    secondform = re.sub(",[-\s0-9]+", "", title)
    if secondform != title and re.search(' ', secondform):
        key = prefix + '-' + urlescape(secondform)
        if key not in db['infolisPattern']:
            created += 1
            db['infolisPattern'][key] = { 
                    'regexPattern': regexify(secondform),
                    '_stringMatch': secondform,
                    'linkTo': [] }
        db['infolisPattern'][key]['linkTo'].append(_id)
    #  # --------------------------
    #  # 3) Abbreviations
    #  # --------------------------
    #  for abbr in re.findall("[A-Z][A-Z]+", title):
    #      if key not in db['infolisPattern']:
    #          db['infolisPattern'][key] = { 
    #                  'regexPattern': regexify(secondform),
    #                  '_stringMatch': secondform,
    #                  'linkTo': [] }
    #      db['infolisPattern'][key]['linkTo'] += _id
    return created
#}}}

#-----------------------------------------------------------------------------
# Do the work
#{{{

def search_patterns_in_files(dbfile, textfiles, metadir):
    bakfile = "/tmp/" + urlescape(dbfile) +"_"+ urlescape(metadir) + ".json"
    with open(dbfile) as jsoninfile:
        db = json.load(jsoninfile)
        cur = 0
        total = len(textfiles)
        total_found = 0
        logging.info("Start Searching %d files" % total)
        t0 = time.time()
        throughput = 0
        for textfile in textfiles:
            found = []
            metafile = metadir + "/" + os.path.splitext(os.path.basename(textfile))[0] + ".xml"
            entity = make_entity_from_oai(metafile)
            db['entity'][entity['_id']] = entity
            infolis_file, textcontents = make_infolis_file_from_textfile(textfile, entity)
            for pat_id in db['infolisPattern']:
                pat = db['infolisPattern'][pat_id]
                #
                # XXX SEARCH IS HERE
                #
                # for efficiency: look for string using 'x in y' syntax first,
                if pat['_stringMatch'] not in textcontents:
                    continue
                if re.search(cachedRegex(pat['regexPattern']), textcontents):
                    found.append(pat_id)
                    make_entity_link_from_pattern(db, entity['_id'], pat_id)
            cur += 1
            #  sys.stderr.write(CLEAR)
            print_progress(cur, total, total_found, t0)
            total_found += len(found)
            if cur % BAK_INTERVAL == 0:
                logging.debug("Saving intermediary results to %s" % bakfile)
                with open(bakfile, 'w') as jsonoutfile:
                    jsonoutfile.write(json.dumps(db, indent=2))
    return db

#}}}

#-----------------------------------------------------------------------------
# CLI Commands
#{{{ 

def search_patterns(dbfile, textdir, metadir, outdbfile):
    textfiles = glob.glob(textdir + "/*.txt")
    logging.info("Number of text files: %d" % len(textfiles))
    db = search_patterns_in_files(dbfile, textfiles, metadir)
    with open(outdbfile, 'w') as jsonoutfile:
        logging.info("Finished matching, writing out")
        jsonoutfile.write(json.dumps(db, indent=2))

def jsonify_dara(darafile, outdbfile):
    context = ET.iterparse(darafile, events=('end',))
    db = { "entity": {}, "infolisPattern": {}, "entityLink": {} }
    cur = 0
    logging.debug("Counting documents in file ... ")
    total = int(subprocess.check_output("grep -o \"<doc>\" %s|wc -l" % (darafile), shell=True))
    logging.debug("... finished counting: %d" % total)
    found = 0
    t0 = time.time()
    for action, elem in context:
        if elem.tag=='doc':
            entity = make_entity_from_solr_doc(db, elem)
            if entity:
                db['entity'][entity['_id']] = entity
                found += make_pattern(db, 'darapat', entity['name'], entity['_id'])
                if '_doi' in entity and entity['_doi'] != None:
                    found += make_pattern(db, 'darapat', entity['_doi'], entity['_id'])
            cur += 1
            print_progress(cur, total, found, t0)
            elem.clear()
    with open(outdbfile, 'w') as jsonoutfile:
        jsonoutfile.write(json.dumps(db, indent=2))

def jsonify_databases(infile, outfile):
    """
    Transform the Google Docs Table CSV to a json-import-able structure
    """
    db = { "entity": {}, "infolisPattern": {}, 'entityLink': {} }
    with io.open(infile, 'r', encoding='utf8') as csvfile:
        total = len(list(csv.reader(open(infile))))
        csvreader = csv.reader(csvfile)
        next(csvreader, None)
        cur = 0
        found = 0
        t0 = time.time()
        for row in csvreader:
            cur += 1
            _id = row[DATABASES_CSV_HEADER['ID']]
            title = row[DATABASES_CSV_HEADER['TITLE']]
            url = row[DATABASES_CSV_HEADER['URL']]
            keywords = row[DATABASES_CSV_HEADER['KEYWORDS']].split(" ; ")
            db['entity'][_id] = {
                "url": url,
                "identifier": url,
                "name": title
            }
            found += make_pattern(db, 'dbpat', title, _id)
            for pat in keywords:
                if pat != '':
                    found += make_pattern(db, 'dbpat', pat, _id)
            print_progress(cur, total, found, t0)
        with open(outfile, mode="w") as jsonfile:
            jsonfile.write(json.dumps(db, indent=2))

def jsonify_icpsr_studies(infile, outfile):
    db = { "entity": {}, "infolisPattern": {}, "entityLink": {} }
    with open(infile, 'r') as csvfile:
        total = len(list(csv.reader(open(infile))))
        csvreader = csv.reader(csvfile)
        next(csvreader, None)
        cur = 0
        found = 0
        t0 = time.time()
        for row in csvreader:
            _id = "icpsr_" + row[ICPSRSTUDIES_CSV_HEADER['STUDY_NUMBER']]
            title = row[ICPSRSTUDIES_CSV_HEADER['TITLE']]
            doi = row[ICPSRSTUDIES_CSV_HEADER['DOI']]
            db['entity'][_id] = {
                'name': title,
                'identifier': doi }
            found += make_pattern(db, 'icpsrpat', title, _id)
            found += make_pattern(db, 'icpsrpat', doi, _id)
            cur += 1
            print_progress(cur, total, found, t0)
        with open(outfile, mode="w") as jsonfile:
            jsonfile.write(json.dumps(db, indent=2, encoding='latin1'))
    pass
#}}}


#-----------------------------------------------------------------------------
# Main
#{{{ 

def print_usage(exit_code):
    prog = sys.argv[0]
    print("Usage: " + prog + " <command> [args...]")
    print("""
    Commands:

    jsonify-databases <csv> <out-json>
        Convert Databases CSV <csv> to JSON

    jsonify-dara <solr-xml> <out-json>
        Convert da-ra solr xml to JSON

    jsonify-icpsr-studies <csv> <out-json>
        Convert ICPSR studies CSV to JSON

    TODO merge-dbs <outjson> <in1> <in2...>
        Merges JSON files to be uploaded or used for search

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
    if cmd == 'jsonify-databases':
        if len(sys.argv) != 4:
            print_usage(1)
        jsonify_databases(sys.argv[2], sys.argv[3])
    elif cmd == 'jsonify-dara':
        if len(sys.argv) != 4:
            print_usage(1)
        jsonify_dara(sys.argv[2], sys.argv[3])
    elif cmd == 'jsonify-icpsr-studies':
        if len(sys.argv) != 4:
            print_usage(1)
        jsonify_icpsr_studies(sys.argv[2], sys.argv[3])
    elif cmd == 'search-patterns':
        if len(sys.argv) != 6:
            print_usage(1)
        search_patterns(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        print_usage(1)
#}}}
