MINER = python dbminer.py
IMAGE = infolis/infolis-dbminer
JSON_TARGETS = import/dara-solr.json import/icpsr-studies.json import/databases.json

RM = rm -f
WGET = wget
CURL = curl -s

pdfbox.jar:
	wget -O$@ "http://mirror.synyx.de/apache/pdfbox/2.0.2/pdfbox-app-2.0.2.jar"

clean:
	$(RM) $(JSON_TARGETS)

#
# Imports
#

import: $(JSON_TARGETS)

import/dara-solr.xml:
	$(WGET) -O$@ "http://www.da-ra.de/solr/dara/select?rows=100000&q=resourceType:2"

import/dara-solr.json: import/dara-solr.xml
	$(MINER) jsonify-dara "$<" "$@"

import/databases.csv:
	echo "Please download as 'CSV (comma-separated)' from https://docs.google.com/spreadsheets/d/1UEp9BsnR5QrHcaBAcJ2znKmWqigWfq_4NFoU9WBtH_0/edit#gid=0"
	exit 1

import/databases.json: import/databases.csv
	$(MINER) jsonify-databases "$<" "$@"

import/icpsr-studies.csv:
	$(CURL) "http://www.icpsr.umich.edu/icpsrweb/ICPSR/csv/studies?collection=DATA&paging.startRow=0&paging.rows=1000&archive=ICPSR" > "$@"
	for i in $$(seq 1000 1000 10000);do \
		$(CURL) "http://www.icpsr.umich.edu/icpsrweb/ICPSR/csv/studies?collection=DATA&paging.startRow=$${i}&paging.rows=1000&archive=ICPSR" |sed -n '2,$$p' >> "$@" ; \
		sleep 1; \
	done
	sed -i -n '/^./p' "$@"

import/icpsr-studies.json: import/icpsr-studies.csv
	$(MINER) jsonify-icpsr-studies "$<" "$@"

#
# Docker
#

docker-build:
	docker build -t $(IMAGE) .

docker-run: build
	docker run --rm -it -v $(PWD)/data:/app/data $(IMAGE)
