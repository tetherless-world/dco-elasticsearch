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


PROV = Namespace("http://www.w3.org/ns/prov#")
BIBO = Namespace("http://purl.org/ontology/bibo/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
DCO = Namespace("http://info.deepcarbon.net/schema#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")

# standard filters
non_empty_str = lambda s: True if s else False
has_label = lambda o: True if o.label() else False


#############################################
#    Some helper functions
#
def load_file(filepath):
    with open(filepath) as _file:
        return _file.read().replace('\n', " ")


def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False


def get_metadata(index, type, id):
    return {"index": {"_index": index, "_type": type, "_id": id}}



###################################################
#    Helper functions to get different attributes
#

def get_id(dco_id):
    return dco_id[dco_id.rfind('/') + 1:]


def get_dco_communities(dataset):
    return Maybe.of(dataset).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOCommunity)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_portal_groups(dataset):
    return Maybe.of(dataset).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOPortalGroup)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_data_types(dataset):
    return Maybe.of(dataset).stream() \
        .flatmap(lambda p: p.objects(DCO.hasDataType)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_cites(dataset):
    return Maybe.of(dataset).stream() \
        .flatmap(lambda p: p.objects(BIBO.cites)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_projects_of_dataset(dataset):
    return Maybe.of(dataset).stream() \
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
        print("missing rank for one or more authors of:", dataset)

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
        # fileList = [f for f in distribution.objects(DCO.hasFile) if has_type(f, DCO.File)]
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
# end

#############################################
#    Implementation of Main(...):
#

def Main(get_objects_query_location, describe_object_query_location,
         create_object_doc_function, object_index, object_type, variable_name_sparql):

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

    ingestSomething = Ingest(object_index=object_index, object_type=object_type,
                             get_objects_query_location=get_objects_query_location,
                             describe_object_query_location=describe_object_query_location,
                             variable_name_sparql=variable_name_sparql)

    # generate bulk import document for datasets
    ingestSomething.generate(threads=int(args.threads), sparql=args.sparql,
                             create_object_doc_function=create_object_doc_function)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(ingestSomething.records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(ingestSomething.records)
        ingestSomething.publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)


#############################################
#    Implementation of create_dataset_doc(...):
#

# describe: helper function for describe_object
def describe(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    try:
        return sparql.query().convert()
    except RuntimeWarning:
        pass

# describe_object: helper function for create_XXXXXXXXXX_doc
def describe_object(endpoint, object, describe_object_query, variable_name_sparql):
    q = describe_object_query.replace(variable_name_sparql, "<" + object + ">")
    return describe(endpoint, q)


# create_dataset_doc: case-varying;
#       passed as an external argument of the function Ingest.generate
#       (then passed to Ingest.process_object)
def create_dataset_doc(dataset, endpoint, describe_object_query, variable_name_sparql):
    graph = describe_object(endpoint=endpoint, object=dataset,
                             describe_object_query=describe_object_query,
                             variable_name_sparql=variable_name_sparql)

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



