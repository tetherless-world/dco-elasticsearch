#!/bin/sh

ofile=`date +"%Y%m%d-%H-%M-%S".datatypes.bulk`

cd /opt/dco/dco-elasticsearch/ingest

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-datatypes.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish ${ofile} >> /var/log/datatypes-ingest.log

gzip ${ofile}
mv ${ofile}.gz /opt/backups/es
