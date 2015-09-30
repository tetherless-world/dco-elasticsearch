# In development of utilizing sepatate .py files for common functions


#########################################################
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Namespace, RDF
import json
import requests
import multiprocessing
import itertools
# from itertools import chain
import functools
import argparse
import warnings
import pprint
#########################################################
from ingestHelpers import *
#########################################################

# Global variables for the ingest process for: ***dataset***
get_datasets_query = load_file("queries/listDatasets.rq")
describe_dataset_query = load_file("queries/describeDataset.rq")
_index = "dco"
_type = "dataset"



def process_dataset(dataset, endpoint):
    ds = create_dataset_doc(dataset=dataset, endpoint=endpoint)
    if "dcoId" in ds and ds["dcoId"] is not None:
        return [json.dumps(get_metadata(_index, _type, (ds["dcoId"]))), json.dumps(ds)]
    else:
        return []


def describe_dataset(endpoint, dataset):
    q = describe_dataset_query.replace("?dataset", "<" + dataset + ">")
    return describe(endpoint, q)


def generate(threads, sparql, get_objects_query):
    pool = multiprocessing.Pool(threads)
    # params = [(dataset, sparql) for dataset in get_datasets(endpoint=sparql)] ==>
    params = [(object, sparql) for object in get_objects(endpoint=sparql, get_objects_query=get_objects_query)]
    return list(itertools.chain.from_iterable(pool.starmap(process_dataset, params)))


def create_dataset_doc(dataset, endpoint):
    graph = describe_dataset(endpoint=endpoint, dataset=dataset)

    ds = graph.resource(dataset)

    try:
        title = ds.label().toPython()
    except AttributeError:
        print("missing title:", dataset)
        return {}

    dco_id = list(ds.objects(DCO.hasDcoId))
    dco_id = str(dco_id[0].identifier) if dco_id else None

    doc = {"uri": dataset, "title": title, "dcoId": dco_id}

    doi = list(ds.objects(BIBO.doi))
    doi = doi[0].toPython() if doi else None
    if doi:
        doc.update({"doi": doi})

    abstract = list(ds.objects(BIBO.abstract))
    abstract = abstract[0].toPython() if abstract else None
    if abstract:
        doc.update({"abstract": abstract})

    most_specific_type = list(ds.objects(VITRO.mostSpecificType))
    most_specific_type = most_specific_type[0].label().toPython() \
        if most_specific_type and most_specific_type[0].label() \
        else None
    if most_specific_type:
        doc.update({"mostSpecificType": most_specific_type})

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

    # projects NOT WORKING YET
    projects = get_projects(ds)
    if projects:
        doc.update({"projects": projects})

    # dataType
    data_types = get_data_types(ds)
    if data_types:
        doc.update({"dataTypes": data_types})

    # cites NOT WORKING YET
    cites = get_cites(ds)
    if cites:
        doc.update({"citations": cites})

    # authors
    authors = get_authors(ds)
    doc.update({"authors": authors})

    # distributions
    distributions = get_distributions(ds)
    doc.update({"distributions": distributions})

    return doc



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=1, help='number of threads to use (default = 8)')
    parser.add_argument('--es', default="http://localhost:9200", help="elasticsearch service URL")
    # parser.add_argument('--es', default="https://dcotest.tw.rpi.edu/search", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/dataset.json", help="dataset elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    # parser.add_argument('--sparql', default='http://udco.tw.rpi.edu/fuseki/vivo/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    args = parser.parse_args()

    # generate bulk import document for datasets
    records = generate(threads=int(args.threads), sparql=args.sparql, get_objects_query=get_datasets_query)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping, index=_index, tYPE=_type)



    # SOME RUNNING SCRIPTS:
    # python3 ingest-datasets.py output
    # GET dco/dataset/_mapping
    # DELETE /dco/dataset/_mapping
    # curl -XPUT 'localhost:9200/dco/dataset/_mapping?pretty' --data-binary @mappings/dataset.json
    # curl -XPOST 'localhost:9200/_bulk' --data-binary @output
