__author__ = 'Hao'

from rdflib import Namespace, RDF
from itertools import chain
import argparse
# import warnings
# import pprint

# Auxilary class for those helper functions getting attributes of objects
from Maybe import *

# Auxilary class implementing the ingest process
from Ingest import *


# standard filters
non_empty_str = lambda s: True if s else False
has_label = lambda o: True if o.label() else False


###################################################
#    Helper functions to get different attributes
#
def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False


def get_id(dco_id):
    return dco_id[dco_id.rfind('/') + 1:]


def get_dco_communities(x):
    return Maybe.of(x).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOCommunity)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_portal_groups(x):
    return Maybe.of(x).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOPortalGroup)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_data_types(x):
    return Maybe.of(x).stream() \
        .flatmap(lambda p: p.objects(DCO.hasDataType)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_cites(x):
    return Maybe.of(x).stream() \
        .flatmap(lambda p: p.objects(BIBO.cites)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_projects_of_dataset(x):
    return Maybe.of(x).stream() \
        .flatmap(lambda p: p.objects(DCO.isDatasetOf)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()


# get_authors: object -> [authors] for objects such as: datasets, publications, ...
def get_authors(ds):
    authors = []
    authorships = [faux for faux in ds.objects(VIVO.relatedBy) if has_type(faux, VIVO.Authorship)]
    for authorship in authorships:

        author = [person for person in authorship.objects(VIVO.relates) if has_type(person, FOAF.Person)][0]
        name = author.label().toPython() if author else None

        obj = {"uri": str(author.identifier), "name": name}

        rank = list(authorship.objects(VIVO.rank))
        rank = str(rank[0].toPython()) if rank else None # added the str()
        if rank:
            obj.update({"rank": rank})

        research_areas = [research_area.label().toPython() for research_area in author.objects(VIVO.hasResearchArea) if research_area.label()]

        if research_areas:
            obj.update({"researchArea": research_areas})

        authors.append(obj)

        org = list(author.objects(DCO.inOrganization))
        org = org[0] if org else None
        if org and org.label():
            obj.update({"organization": {"uri": str(org.identifier), "name": org.label().toPython()}})

    try:
        authors = sorted(authors, key=lambda a: a["rank"]) if len(authors) > 1 else authors
    except KeyError:
        print("missing rank for one or more authors of:", ds)

    return authors


# get_distributions: object -> [distributions] for objects such as: datasets, publications, ...
def get_distributions(ds):
    distributions = []
    distributionList = [faux for faux in ds.objects(DCO.hasDistribution) if has_type(faux, DCAT.Distribution)]
    for distribution in distributionList:
        accessURL = str(list(distribution.objects(DCO.accessURL))[0])
        name = distribution.label().toPython() if distribution else None
        obj = {"uri": str(distribution.identifier), "accessURL": accessURL, "name": name}

        fileList = list(distribution.objects(DCO.hasFile))
        fileList = fileList if fileList else None
        files = []
        for file in fileList:
            downloadURL = list(file.objects(DCO.downloadURL))
            downloadURL = str(downloadURL[0]) if downloadURL else None
            fileObj = {"uri": str(file.identifier),
                       "name": file.label().toPython()}
            fileObj.update({"downloadURL": downloadURL})
            files.append(fileObj)

        if files:
            obj.update({"files": files})

        distributions.append(obj)

    return distributions


#############################################
#    Implementation of main(...):
#
def main(get_objects_query_location, describe_object_query_location,
         elasticsearch_index, elasticsearch_type, variable_name_sparql, XIngest):

    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=4, help='number of threads to use (default = 4)')
    parser.add_argument('--es', default="http://localhost:9200", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/dataset.json", help="dataset elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    # Info:
    #   local elasticsearch URL:          http://localhost:9200
    #   dcotest elasticsearch URL:        https://dcotest.tw.rpi.edu/search
    #   production sparql endpoint:       http://deepcarbon.tw.rpi.edu:3030/VIVO/query
    #   test sparql endpoint:             http://udco.tw.rpi.edu/fuseki/vivo/query

    args = parser.parse_args()

    ingestSomething = XIngest(elasticsearch_index=elasticsearch_index, elasticsearch_type=elasticsearch_type,
                             get_objects_query_location=get_objects_query_location,
                             describe_object_query_location=describe_object_query_location,
                             variable_name_sparql=variable_name_sparql)

    # generate bulk import document for xs
    ingestSomething.generate(threads=int(args.threads), sparql=args.sparql)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(ingestSomething.records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(ingestSomething.records)
        ingestSomething.publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)