__author__ = 'Hao'

from rdflib import Namespace, RDF
import requests


from Maybe import *

from SPARQLWrapper import SPARQLWrapper, JSON
import json
import multiprocessing
import itertools
# from itertools import chain
import functools
import argparse
import warnings
import pprint



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

def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False


def get_metadata(index, type, id):
    return {"index": {"_index": index, "_type": type, "_id": id}}


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




def publish(bulk, endpoint, rebuild, mapping, index, tYPE):
    # if configured to rebuild_index
    # Delete and then re-create to publication index (via PUT request)

    index_url = endpoint + "/" + index

    if rebuild:
        requests.delete(index_url)
        r = requests.put(index_url)
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()

    # push current publication document mapping

    mapping_url = index_url + "/" + tYPE + "/_mapping"
    with open(mapping) as mapping_file:
        r = requests.put(mapping_url, data=mapping_file)
        if r.status_code != requests.codes.ok:

            # new mapping may be incompatible with previous
            # delete current mapping and re-push

            requests.delete(mapping_url)
            r = requests.put(mapping_url, data=mapping_file)
            if r.status_code != requests.codes.ok:
                print(r.url, r.status_code)
                r.raise_for_status()

    # bulk import new publication documents
    bulk_import_url = endpoint + "/_bulk"
    r = requests.post(bulk_import_url, data=bulk)
    if r.status_code != requests.codes.ok:
        print(r.url, r.status_code)
        r.raise_for_status()

####################################################
##
####################################################

def load_file(filepath):
    with open(filepath) as _file:
        return _file.read().replace('\n', " ")



def select(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


# get_datasets -> get_objects
def get_objects(endpoint, get_objects_query, object_type):
    r = select(endpoint, get_objects_query)
    # return [rs["dataset"]["value"] for rs in r]
    return [rs[object_type]["value"] for rs in r]


def describe(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    try:
        return sparql.query().convert()
    except RuntimeWarning:
        pass


# process_dataset: used by generate
def process_dataset(dataset, endpoint, create_object_doc_function, object_index, object_type):
    # ds = create_dataset_doc(dataset=dataset, endpoint=endpoint) ==>
    ds = create_object_doc_function(dataset=dataset, endpoint=endpoint)
    if "dcoId" in ds and ds["dcoId"] is not None:
        return [json.dumps(get_metadata(object_index, object_type, (ds["dcoId"]))), json.dumps(ds)]
    else:
        return []


def generate(threads, sparql, get_objects_query, process_object_function, create_object_doc_function, object_index, object_type):
    pool = multiprocessing.Pool(threads)
    # params = [(dataset, sparql) for dataset in get_datasets(endpoint=sparql)] ==>
    params = [(object, sparql, create_object_doc_function, object_index, object_type) for object in get_objects(endpoint=sparql, get_objects_query=get_objects_query, object_type=object_type)]
    return list(itertools.chain.from_iterable(pool.starmap(process_object_function, params)))