#!/bin/sh

##############
# People
##############

echo "**** Start People Ingest"
ofile=`date +"%Y%m%d-%H-%M-%S".people.bulk`

cd /opt/dco/dco-elasticsearch/ingest

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-people.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish ${ofile} >> /var/log/people-ingest.log

gzip ${ofile}
mv ${ofile}.gz /opt/backups/es
echo "**** End People Ingest"
sleep 30

##############
# Projects
##############

echo "**** Start Project Ingest"
ofile=`date +"%Y%m%d-%H-%M-%S".projects.bulk`

cd /opt/dco/dco-elasticsearch/ingest 

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-projects.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish $ofile >> /var/log/projects-ingest.log 

gzip $ofile
mv $ofile.gz /opt/backups/es/
echo "**** End Project Ingest"
sleep 30

##############
# Publications
##############

echo "**** Start Publications Ingest"
ofile=`date +"%Y%m%d-%H-%M-%S".publications.bulk`

cd /opt/dco/dco-elasticsearch/ingest 

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-publications.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish $ofile >> /var/log/publication-ingest.log

gzip $ofile
mv $ofile.gz /opt/backups/es/
echo "**** End Publications Ingest"

##############
# Field Study
##############

echo "**** Start Field Study Ingest"

ofile=`date +"%Y%m%d-%H-%M-%S".fieldstudy.bulk`

cd /opt/dco/dco-elasticsearch/ingest 

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-field-studies.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish $ofile >> /var/log/fieldstudy-ingest.log

gzip $ofile

mv $ofile.gz /opt/backups/es/

##############
# Dataset
##############

echo "**** Start Dataset Ingest"
ofile=`date +"%Y%m%d-%H-%M-%S".dataset.bulk`

cd /opt/dco/dco-elasticsearch/ingest 

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-datasets.py --threads 4 --sparql http://localhost:2020/vivo/query --es
http://localhost:49200 --publish $ofile >> /var/log/dataset-ingest.log

gzip $ofile

mv $ofile.gz /opt/backups/es/

##############
# Data Types
##############

echo "**** Start Data Type Ingest"
ofile=`date +"%Y%m%d-%H-%M-%S".datatypes.bulk`

cd /opt/dco/dco-elasticsearch/ingest

/usr/bin/python3 /opt/dco/dco-elasticsearch/ingest/ingest-datatypes.py --threads 4 --sparql http://localhost:2020/vivo/query --es http://localhost:49200 --publish ${ofile} >> /var/log/datatypes-ingest.log

gzip ${ofile}
mv ${ofile}.gz /opt/backups/es

##############
# Cleanup
##############

cd /opt/backups/es
find . -atime +5 -delete

