# dco-elasticsearch
DCO Faceted Search using ElasticSearch

## Overview

The DCO faceted search browsers are powered by [FacetView2](https://github.com/tetherless-world/facetview2) - a pure javascript frontend for ElasticSearch search indices that lets you easily embed a faceted browse front end into any web page.

To configure a working faceted browser you need:
1) A running instance of ElasticSearch with a populated index
2) A webpage with references to facetview2 scripts and an embedded configuration

That's it!

You can find out more about ElasticSearch at [https://www.elastic.co/products/elasticsearch](https://www.elastic.co/products/elasticsearch)

### The Ingest Process

Currently we use python scripts to build and import batch documents into our ElasticSearch (ES) service.  The scripts can be scheduled or run manually and currently generate a batch of all documents for a given search type (e.g. publication, person, dataset, etc).  Our scripts do not currently support generating batches of only recently changed documents.  That ingest scripts query a DCO SPARQL endpoint and generate a ES batch file containing JSON documents for all entities to be indexed.

Documentation on the ES batch input format is available at [https://www.elastic.co/guide/en/elasticsearch/reference/current/_batch_processing.html](https://www.elastic.co/guide/en/elasticsearch/reference/current/_batch_processing.html).

The ingest scripts will perform the import when the --publish command line is specified.  You can change the elasticsearch URL the script imports to with the --es command line parameter.

!IMPORTANT! - if you are writing a new ingest script make sure the correct values for the ``_index`` and ``_type`` variables are specified at the top of the script.

To see the full list of commandline options available with the ingest script use -h.

Our current batch scripts take between 3-5 minutes each to run, generate 2-4MB batch files, and import into ES usually takes a couple seconds.

### Importing data into ElasticSearch

Elasticsearch is a search server based on Lucene. It provides a distributed, multitenant-capable full-text search engine with a RESTful web interface and schema-free JSON documents.

For example,

```json
{
  "name": "John Doe",
  "network_id": "jdoe",
  "email": "jdoe@example.com",
  "interests": ["fishing","kayaking","golf"],
  ""
}
```

Detailed documentation is available at [https://www.elastic.co/guide/en/elasticsearch/guide/master/index.html](https://www.elastic.co/guide/en/elasticsearch/guide/master/index.html)

#### Bulk document import

#### custom mapping import

### Setting up the faceted browser

```js
<script type="text/javascript">
jQuery(document).ready(function($) {
  $('.facet-view-simple').facetview({
    search_url: 'http://localhost:9200/myindex/type/_search',
    facets: [
        {'field': 'publisher.exact', 'size': 100, 'order':'term', 'display': 'Publisher'},
        {'field': 'author.name.exact', 'display': 'author'},
        {'field': 'year.exact', 'display': 'year'}
    ],
  });
});
</script>
```

Then add a ``div`` with the HTML class referenced in the script to the HTML body.
```
</head>
    <body>
        <div class="facet-view-simple"></div>
    </body>
</html>
```