#!/usr/bin/env coffee

Fs = require 'fs'
CsvParse = require 'csv-parse'

argv = process.argv.slice 2
unless argv[0]
	argv[0] = './import.tsv'
db =
	infolisPattern: {}
	entity: {}

SPLITTER=/\s;\s/
TAG='infolis-dbminer'
ID='Identifier'
TITLE='Titel'
PATTERNS='Kurzform'
URL='URL'

urlescape = (str) -> str.replace /[^0-9a-bA-Z]/ig, '-'

Fs.createReadStream(argv[0]).pipe(
	CsvParse(delimiter:'\t', columns: true)
).on('data', (row) ->
	if row[PATTERNS]
		for pattern in row[PATTERNS]?.split SPLITTER
			db.infolisPattern["#{TAG}-#{urlescape pattern}"] =
				patternRegex: pattern
				alwaysLinkTo: row[ID]
				tag: ['infolis-db-ontology']
	db.entity[row[ID]] =
		identifier: row[URL]
		name: row[TITLE]
		tag: ['infolis-db-ontology']
).on('end', ->
	console.log JSON.stringify db, null, 2
)
