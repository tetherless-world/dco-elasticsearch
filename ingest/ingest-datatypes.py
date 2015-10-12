__author__ = 'szednik'
#Edited by Ahmed (am-e) to ingest dataTypes

from SPARQLWrapper import SPARQLWrapper, JSON
import json
from rdflib import Namespace, RDF
import multiprocessing
import itertools
from itertools import chain
import argparse
import requests
import warnings
import pprint


class Maybe:
    def __init__(self, v=None):
        self.value = v

    @staticmethod
    def nothing():
        return Maybe()

    @staticmethod
    def of(t):
        return Maybe(t)

    def reduce(self, action):
        return Maybe.of(functools.reduce(action, self.value)) if self.value else Maybe.nothing()

    def stream(self):
        return Maybe.of([self.value]) if self.value is not None else Maybe.nothing()

    def map(self, action):
        return Maybe.of(chain(map(action, self.value))) if self.value is not None else Maybe.nothing()

    def flatmap(self, action):
        return Maybe.of(chain.from_iterable(map(action, self.value))) if self.value is not None else Maybe.nothing()

    def andThen(self, action):
        return Maybe.of(action(self.value)) if self.value is not None else Maybe.nothing()

    def orElse(self, action):
        return Maybe.of(action()) if self.value is None else Maybe.of(self.value)

    def do(self, action):
        if self.value:
            action(self.value)
        return self

    def filter(self, action):
        return Maybe.of(filter(action, self.value)) if self.value is not None else Maybe.nothing()

    def followedBy(self, action):
        return self.andThen(lambda _: action)

    def one(self):
        try:
            return Maybe.of(next(self.value)) if self.value is not None else Maybe.nothing()
        except StopIteration:
            return Maybe.nothing()
        except TypeError:
            return self

    def list(self):
        return list(self.value) if self.value is not None else []


def load_file(filepath):
    with open(filepath) as _file:
        return _file.read().replace('\n', " ")


# Global variables for the ingest process for: ***dataType***
get_dataTypes_query = load_file("queries/listdataTypes.rq")
describe_dataType_query = load_file("queries/describedataType.rq")

PROV = Namespace("http://www.w3.org/ns/prov#")
BIBO = Namespace("http://purl.org/ontology/bibo/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
VITRO_PUB = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/public#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
DCO = Namespace("http://info.deepcarbon.net/schema#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
PROV = Namespace("http://www.w3.org/ns/prov#")


# get_metadata: returns the index and type of the specified id
def get_metadata(id):
    return {"index": {"_index": "dco", "_type": "datatype", "_id": id}}


# get_id: returns dcoId of entity being ingested
def get_id(dco_id):
    return dco_id[dco_id.rfind('/') + 1:]


# select: run the supplied SPARQL SELECT query
def select(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]

# describe: run the supplied SPARQL DESCRIBE query
def describe(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    try:
        return sparql.query().convert()
    except RuntimeWarning:
        pass

# get_dataTypes: run the dataTypes SELECT query and return the result set
def get_dataTypes(endpoint):
    r = select(endpoint, get_dataTypes_query)
    return [rs["dataType"]["value"] for rs in r]


# process_dataType: used by generate
def process_dataType(dataType, endpoint):
    dt = create_dataType_doc(dataType=dataType, endpoint=endpoint)
    if "dcoId" in dt and dt["dcoId"] is not None:
        return [json.dumps(get_metadata(get_id(dt["dcoId"]))), json.dumps(dt)]
    else:
        return []

# describe_dataType: used by create_projcet_doc
# change the "?dataType" variable to whatever variable name you use in the listXXX.rq file
def describe_dataType(endpoint, dataType):
    q = describe_dataType_query.replace("?dataType", "<" + dataType + ">")
    return describe(endpoint, q)

# create_dataType_doc: used by process_dataType
# creates a document to insert into elasticsearch based on the mapping in mappings/dataType.json
def create_dataType_doc(dataType, endpoint):
    graph = describe_dataType(endpoint=endpoint, dataType=dataType)

    dt = graph.resource(dataType)

    try:
        title = dt.label().toPython()
    except AttributeError:
        print("missing title:", dataType)
        return {}

    # dcoId of this dataType
    dco_id = list(dt.objects(DCO.hasDcoId))
    dco_id = str(dco_id[0].identifier) if dco_id else None

    doc = {"uri": dataType, "title": title, "dcoId": dco_id}

    # creation year of this dataType
    creation_year = list(dt.objects(DCO.createdAtTime))
    creation_year = creation_year[0].toPython() \
        if creation_year and creation_year[0] \
        else None
    if creation_year:
        doc.update({"creationYear": creation_year})

    # source datatype of this dataType
    source_datatype = list(dt.objects(DCO.sourceDataType))
    source_datatype_label = source_datatype[0].label().toPython() \
        if source_datatype and source_datatype[0].label() \
        else None
    if source_datatype:
        source_datatype_obj = {"uri": str(source_datatype[0].identifier), "title": source_datatype_label}
        doc.update({"sourceDataType": source_datatype_obj})

    # source standard of this dataType
    source_standard = list(dt.objects(DCO.sourceStandard))
    source_standard_label = source_standard[0].label().toPython() \
        if source_standard and source_standard[0].label() \
        else None
    if source_standard:
        source_standard_obj = {"uri": str(source_standard[0].identifier), "title": source_standard_label}
        doc.update({"sourceStandard": source_standard_obj})

    # author(s) of this dataType
    authorsArr = []
    authors = [faux for faux in dt.objects(PROV.wasAttributedTo) if has_type(faux, PROV.Agent)]

    if authors:
        for author in authors:
            #print(community)
            name = author.label().toPython() if author else None

            obj = {"uri": str(author.identifier), "name": name}

            authorsArr.append(obj)

    doc.update({"authors": authorsArr})

    subjectAreasArr = []
    subjectAreas = [faux for faux in dt.objects(DCO.dataTypeSubjectArea)]
    #print(subjectAreas)
    if subjectAreas:
        for subjectArea in subjectAreas:
            print(subjectArea)
            title = subjectArea.label().toPython() if subjectAreas else None

            obj = {"uri": str(subjectArea.identifier), "title": title}

            subjectAreasArr.append(obj)

    doc.update({"subjectAreas": subjectAreasArr})


    return doc

# has_type: asserts whether a resource if of a certain type
def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False

# publish: publishes extracted data to elasticsearch node
def publish(bulk, endpoint, rebuild, mapping):
    # if configured to rebuild_index
    # Delete and then re-create to dataType index (via PUT request)

    index_url = endpoint + "/dco"

    if rebuild:
        requests.delete(index_url)
        r = requests.put(index_url)
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()

    # push current dataType document mapping

    mapping_url = endpoint + "/dco/datatype/_mapping"
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

    # bulk import new dataType documents
    bulk_import_url = endpoint + "/_bulk"
    r = requests.post(bulk_import_url, data=bulk)
    if r.status_code != requests.codes.ok:
        print(r.url, r.status_code)
        r.raise_for_status()

# generate: startes the ingest process
def generate(threads, sparql):
    pool = multiprocessing.Pool(threads)
    params = [(dataType, sparql) for dataType in get_dataTypes(endpoint=sparql)]
    return list(itertools.chain.from_iterable(pool.starmap(process_dataType, params)))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=2, help='number of threads to use (default = 8)')
    parser.add_argument('--es', default="http://data.deepcarbon.net/es", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/datatype.json", help="dataType elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    #parser.add_argument('--sparql', default='http://udco.tw.rpi.edu/fuseki/vivo/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    args = parser.parse_args()

    # generate bulk import document for dataTypes
    records = generate(threads=int(args.threads), sparql=args.sparql)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)


########################################
#
# SOME USEFUL SCRIPTS:
#
# ./bin/elasticsearch
#
# python3 ingest-datatypes.py output
# GET dco/datatype/_mapping
# DELETE /dco/datatype/
# DELETE /dco/datatype/_mapping
# curl -XPUT 'data.deepcarbon.net/es/dco/datatype/_mapping?pretty' --data-binary @mappings/datatype.json
# curl -XPOST 'data.deepcarbon.net/es/_bulk' --data-binary @output
#
#########################################
