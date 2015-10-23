# Usage and Examples of Running Your Ingest Process

## Usage:
    Arguments:
    --threads: number of threads to use (default = 8)
    --es', elasticsearch service URL (default="http://localhost:9200")
    --publish', publish to elasticsearch? (default=False)
    --rebuild', rebuild elasticsearch index? (default=False)
    --mapping', dataset elasticsearch mapping document (default="mappings/dataset.json")
    --sparql', sparql endpoint (default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query')
    [out]: file name of the elasticsearch bulk ingest file

    #### e.g.

        `python3 ingest-datasets-old-2.py output4 --threads 2 --mapping mappings/XXXXX.json ...`


## Line command examples for the ingest process:

0. To start elastic search: [elastic search folder]/bin/elasticsearch

1. (CAUTION!) Delete existing data to avoid uploading error due to mismatching:
      *(For localhost) curl -XDELETE 'localhost:9200/dco/dataset'
      *(For dcotest)   curl -XDELETE 'dcotest.tw.rpi.edu:49200/dco/dataset'

2. Manually upload mapping:
      *(For localhost) curl -XPUT 'localhost:9200/dco/dataset/_mapping?pretty' --data-binary @mappings/dataset.json
      *(For dcotest)   curl -XPUT 'dcotest.tw.rpi.edu:49200/dco/dataset/_mapping?pretty' --data-binary @mappings/dataset.json

3. Generate bulk data:
      *python3 ingest-datasets-old-2.py output#
  and then upload bulk data manually
      *(For localhost) curl -XPOST 'localhost:9200/_bulk' --data-binary @output#
      *(For dcotest)   curl -XPOST 'dcotest.tw.rpi.edu:49200/_bulk' --data-binary @output#

3'. Generate bulk data and upload bulk data automatically:
      *(For localhost) python3 ingest-datasets-old-2.py --es 'localhost:9200' --publish output#
      *(For dcotest)   python3 ingest-datasets-old-2.py --es 'dcotest.tw.rpi.edu/search/' --publish output#

4. To view and operate in Sense:
      - GET dco/dataset/_mapping
      - GET dco/dataset/_search
      - DELETE /dco/dataset/
      - DELETE /dco/dataset/_mapping
      - and etc...