import functools
import argparse
import warnings
import pprint
import pydoc


# Import helper functions from ingestHelpers.py;
# see ingestHelpers.py for details of functions and classes used.
from ingestHelpers import *
from Ingest import *

# Please follow the comments below to create, customize and run the ingest process in you case.

# Start by create a copy of this script and rename it as appropriate. A uniform nomenclature is
# ingest-x.py where x is the plural form of the 'type' of search document generated.

# First, change these case-varying variables below for: dataset ingest

GET_OBJECTS_QUERY_LOCATION = "queries/listDatasets.rq"
DESCRIBE_DATASET_OBJECT_LOCATION = "queries/describeDataset.rq"
INDEX = "dco"
TYPE = "dataset"
VARIABLE_NAME_SPARQL = "?dataset"

# Second, extend the Ingest base class to class 'XIngest' below, where X is the singular form, with capitalized
# initial letter, of the 'type' of search document generated. E.g. DatasetIngest, ProjectIngest, etc.
# Overwrite the subclass attribute 'MAPPING' and the create_x_doc method with appropriate implementations.
# (Existing examples are helpful.)

class DatasetIngest(Ingest):

    MAPPING = "mappings/dataset.json"

    def create_x_doc(self, x, endpoint, describe_object_query, variable_name_sparql):
        graph = self.describe_entity(endpoint=endpoint, entity=x,
                                     describe_object_query=describe_object_query,
                                     variable_name_sparql=variable_name_sparql)

        ds = graph.resource(x)

        try:
            title = ds.label().toPython()
        except AttributeError:
            print("missing title:", x)
            return {}

        dco_id = list(ds.objects(DCO.hasDcoId))
        dco_id = str(dco_id[0].identifier) if dco_id else None

        doc = {"uri": x, "title": title, "dcoId": dco_id}

        most_specific_type = list(ds.objects(VITRO.mostSpecificType))
        most_specific_type = most_specific_type[0].label().toPython() \
            if most_specific_type and most_specific_type[0].label() \
            else None
        if most_specific_type:
            doc.update({"mostSpecificType": most_specific_type})

        doi = list(ds.objects(BIBO.doi))
        doi = doi[0].toPython() if doi else None
        if doi:
            doc.update({"doi": doi})

        abstract = list(ds.objects(BIBO.abstract))
        abstract = abstract[0].toPython() if abstract else None
        if abstract:
            doc.update({"abstract": abstract})

        publication_year = list(ds.objects(DCO.yearOfPublication))
        publication_year = publication_year[0] if publication_year else None
        if publication_year:
            doc.update({"publicationYear": str(publication_year)})

        dco_communities = get_dco_communities(ds)
        if dco_communities:
            doc.update({"dcoCommunities": dco_communities})

        # portal_groups
        portal_groups = get_portal_groups(ds)
        if portal_groups:
            doc.update({"portalGroups": portal_groups})

        # projects
        projects = get_projects_of_dataset(ds)
        if projects:
            doc.update({"projects": projects})

        # dataType
        data_types = get_data_types(ds)
        if data_types:
            doc.update({"dataTypes": data_types})

        # cites
        cites = get_cites(ds)
        if cites:
            doc.update({"citations": cites})

        # authors: if none, will return an empty list []
        authors = get_authors(ds)
        doc.update({"authors": authors})

        # distributions: if none, will return an empty list []
        distributions = get_distributions(ds)
        doc.update({"distributions": distributions})

        return doc


# Third, pass the name of the sub-class just created above to argument 'XIngest=' below in the usage of main().
#       E.g. main(..., XIngest=DatasetIngest)
if __name__ == "__main__":
    main(get_objects_query_location=GET_OBJECTS_QUERY_LOCATION,
         describe_object_query_location=DESCRIBE_DATASET_OBJECT_LOCATION,
         elasticsearch_index=INDEX, elasticsearch_type=TYPE,
         variable_name_sparql=VARIABLE_NAME_SPARQL, XIngest=DatasetIngest)

# Finally, run the ingest process. See detailed usage and examples in ingest/README.md.

