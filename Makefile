IMAGE = infolis/infolis-dbminer

pdfbox.jar:
	wget -O$@ "http://mirror.synyx.de/apache/pdfbox/2.0.2/pdfbox-app-2.0.2.jar"

build:
	docker build -t $(IMAGE) .

run: build
	docker run --rm -it -v $(PWD)/data:/app/data $(IMAGE)
