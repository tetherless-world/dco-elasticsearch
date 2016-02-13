__author__ = 'Hao'

from rdflib import Namespace, RDF
from itertools import chain
import argparse
from SPARQLWrapper import SPARQLWrapper, JSON
import json

from rdflib import Namespace, RDF
PROV = Namespace("http://www.w3.org/ns/prov#")
BIBO = Namespace("http://purl.org/ontology/bibo/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
DCO = Namespace("http://info.deepcarbon.net/schema#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")

# Auxilary class for those helper functions getting attributes of objects
from Maybe import *

# standard filters
non_empty_str = lambda s: True if s else False
has_label = lambda o: True if o.label() else False

def load_file(filepath):
    """
    Helper function to load the .rq files and return a String object w/ replacing '\n' by ' '.
    :param filepath:    file path
    :return:            file content in string format
    """
    with open(filepath) as _file:
        return _file.read().replace('\n', " ")

def sparql_select(endpoint, query):
    """
    Helper function used to run a sparql select query
    :param endpoint:    SPARQL endpoint
    :param query:       the SPARQL query to get the list of objects
    :return:
        a list of objects with its type and uri values, e.g.
            [{'dataset': {'value': 'http://...', 'type': 'uri'}}, ...]
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]

# describe: helper function for describe_entity
def sparql_describe( endpoint, query ):
    """
    Helper function used to run a sparql describe query
    :param endpoint:    SPARQL endpoint
    :param query:       the describe query to run
    :return:
        a json object representing the entity
    """
    sparql = SPARQLWrapper( endpoint )
    sparql.setQuery( query )
    try:
        return sparql.query().convert()
    except RuntimeWarning:
        pass

def get_id( es_id ):
    return dco_id[dco_id.rfind('/') + 1:]

def get_metadata( es_index, es_type, es_id ):
    """
    Helper function to create the JSON string of the metadata of an entity.
    :param id:      unique identifier of the entity
    :return:
        a JSON-format string representing the metadata information of the object,
            e.g. {"index": {"_id": "http://...", "_type": "dataset", "_index": "dco"}}
    """
    return {"index": {"_index": es_index, "_type": es_type, "_id": get_id( es_id )}}

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

def get_teams(x):
    return Maybe.of(x).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOTeam)) \
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

