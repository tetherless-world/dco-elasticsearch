import functools
import argparse
import warnings
import pprint


# Import helper functions from ingestHelpers.py;
# see ingestHelpers.py for details of functions and classes used.
from ingestHelpers import *

# Please follow the comments below to create, customize and run the ingest process in you case.

# Start by create a copy of this script and rename it as appropriate. A uniform nomenclature is
# ingest-x.py where x is the plural form of the 'type' of search document generated.

# First, change these case-varying variables below for: dataset ingest

GET_OBJECTS_QUERY_LOCATION = "queries/listDatasets.rq"
DESCRIBE_DATASET_OBJECT_LOCATION = "queries/describeDataset.rq"
INDEX = "dco"
TYPE = "dataset"
VARIABLE_NAME_SPARQL = "?dataset"

# Second, change the value of variable create_object_doc_function in the usage of Main below
# as appropriate, say, create_x_doc, where x is the single form of the 'type' of search document generated.
# Then implement function create_x_doc in ingestHelpers.py. (Existing examples are helpful.)
if __name__ == "__main__":
    Main(get_objects_query_location=GET_OBJECTS_QUERY_LOCATION,
         describe_object_query_location=DESCRIBE_DATASET_OBJECT_LOCATION,
         create_object_doc_function=create_dataset_doc,  # Need to create case-varying create_xxxxxx_doc in helper file
         object_index=INDEX, object_type=TYPE,
         variable_name_sparql=VARIABLE_NAME_SPARQL)

# Finally, run the ingest process. See detailed usage and examples in ingest/README.md.