SCRIPT = python dbminer.py
IMAGE = infolis/infolis-dbminer

pdfbox.jar:
	wget -O$@ "http://mirror.synyx.de/apache/pdfbox/2.0.2/pdfbox-app-2.0.2.jar"

import/dara-solr.xml:
	wget -O$@ "http://www.da-ra.de/solr/dara/select?rows=100000&q=resourceType:2"

import/dara-solr.json: import/dara-solr.xml
	$(SCRIPT) jsonify-dara "$<" "$@"

import/databases.tsv:
	echo "Please download as 'CSV (tab-separated)' from https://docs.google.com/spreadsheets/d/1UEp9BsnR5QrHcaBAcJ2znKmWqigWfq_4NFoU9WBtH_0/edit#gid=0"
	exit 1

import/icpsr-studies.csv:
	rm -f "$@"
	for i in $$(seq 0 1000 1000);do \
		curl -s "http://www.icpsr.umich.edu/icpsrweb/ICPSR/csv/studies?collection=DATA&paging.startRow=$${i}&paging.rows=1000&archive=ICPSR" \
			| sed -n '2,$$ p' \
			| >> "$@" \
			; \
		sleep 1; \
	done

import/databases.json: import/databases.tsv
	$(SCRIPT) jsonify-databases "$<" "$@"

docker-build:
	docker build -t $(IMAGE) .

docker-run: build
	docker run --rm -it -v $(PWD)/data:/app/data $(IMAGE)
