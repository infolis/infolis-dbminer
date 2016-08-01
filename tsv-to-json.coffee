#!/usr/bin/env coffee

Fs = require 'fs'
CsvParse = require 'csv-parse'

argv = process.argv.slice 2
output = entities: {}

ID='Identifier'
TITLE='Titel'
PATTERNS='Kurzform'
URL='URL'

Fs.createReadStream(argv[0]).pipe(
	CsvParse(delimiter:'\t', columns: true)
).on('data', (row) ->
	# console.log row[ID]
	output.entities[row[ID]] =
		foo: 2
		identifier: row[URL]
		name: row[TITLE]
).on('end', ->
	console.log output
)
# CsvParse.parse argv[0], {delimiter: ','}, (err, data) ->
#   console.log data
#   # console.log Object.keys data[1]
#   # data = data.slice(1)
