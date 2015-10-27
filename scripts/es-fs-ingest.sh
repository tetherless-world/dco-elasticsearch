#!/bin/sh

ofile=`date +"%Y%m%d-%H-%M-%S".fieldstudy.bulk`

cd /opt/dco/dco-elasticsearch/ingest 

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-field-studies.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish $ofile >> /var/log/fieldstudy-ingest.log

gzip $ofile

mv $ofile.gz /opt/backups/es/
