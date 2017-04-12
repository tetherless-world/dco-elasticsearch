__author__ = 'szednik'
#Edited by Ahmed (am-e) to ingest instruments

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


# Global variables for the ingest process for: ***instrument***
get_instruments_query = load_file("queries/listInstruments.rq")
describe_instrument_query = load_file("queries/describeInstrument.rq")

PROV = Namespace("http://www.w3.org/ns/prov#")
BIBO = Namespace("http://purl.org/ontology/bibo/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
VITRO_PUB = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/public#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
DCO = Namespace("http://info.deepcarbon.net/schema#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")


# get_metadata: returns the index and type of the specified id
def get_metadata(id):
    return {"index": {"_index": "dco", "_type": "instrument", "_id": id}}


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

# get_instruments: run the instruments SELECT query and return the result set
def get_instruments(endpoint):
    r = select(endpoint, get_instruments_query)
    return [rs["instrument"]["value"] for rs in r]


# process_instrument: used by generate
def process_instrument(instrument, endpoint):
    ins = create_instrument_doc(instrument=instrument, endpoint=endpoint)
    if "dcoId" in ins and ins["dcoId"] is not None:
        return [json.dumps(get_metadata(get_id(ins["dcoId"]))), json.dumps(ins)]
    else:
        return []

# describe_instrument: used by create_projcet_doc
# change the "?instrument" variable to whatever variable name you use in the listXXX.rq file
def describe_instrument(endpoint, instrument):
    q = describe_instrument_query.replace("?instrument", "<" + instrument + ">")
    return describe(endpoint, q)

def get_thumbnail(instrument):
    return Maybe.of(instrument).stream() \
        .flatmap(lambda p: p.objects(VITRO_PUB.mainImage)) \
        .flatmap(lambda i: i.objects(VITRO_PUB.thumbnailImage)) \
        .flatmap(lambda t: t.objects(VITRO_PUB.downloadLocation)) \
        .map(lambda t: t.identifier) \
        .one().value

# create_instrument_doc: used by process_instrument
# creates a document to insert into elasticsearch based on the mapping in mappings/instrument.json
def create_instrument_doc(instrument, endpoint):
    graph = describe_instrument(endpoint=endpoint, instrument=instrument)

    ins = graph.resource(instrument)

    try:
        title = ins.label().toPython()
    except AttributeError:
        print("missing title:", instrument)
        return {}

    #dcoId of this instrument
    dco_id = list(ins.objects(DCO.hasDcoId))
    dco_id = str(dco_id[0].label().toPython()) if dco_id else None

    doc = {"uri": instrument, "title": title, "dcoId": dco_id}

    #type of this instrument: Instrument/Field Study
    most_specific_type = list(ins.objects(VITRO.mostSpecificType))
    most_specific_type = most_specific_type[0].label().toPython() \
        if most_specific_type and most_specific_type[0].label() \
        else None
    if most_specific_type:
        doc.update({"mostSpecificType": most_specific_type})

    #instrument description
    description = list(ins.objects(VIVO.description))
    description = description[0] if description else None
    if description:
        doc.update({"description": str(description)})

    #initiative of this instrument
    initiative = list(ins.objects(DCO.builtDuringInitiative))
    initiative = initiative[0] if initiative else None
    if initiative and initiative.label():
        doc.update({"initiative": {"uri": str(initiative.identifier), "name": initiative.label().toPython()}})
    elif initiative:
        print("initiative label missing:", str(initiative.identifier))

    #org of this instrument
    org = list(ins.objects(VIVO.equipmentFor))
    org = org[0] if org else None
    if org and org.label():
        doc.update({"org": {"uri": str(org.identifier), "name": org.label().toPython()}})
    elif org:
        print("org label missing:", str(org.identifier))    

    #dcoCommunities associated with this instrument
    associatedCommunities = []
    communities = [faux for faux in ins.objects(DCO.associatedDCOCommunity) if has_type(faux, DCO.ResearchCommunity)]

    if communities:
        for community in communities:
            #print(community)
            name = community.label().toPython() if community else None

            obj = {"uri": str(community.identifier), "name": name}

            associatedCommunities.append(obj)

    doc.update({"dcoCommunities": associatedCommunities})

    #project of this instrument
    project = list(ins.objects(DCO.instrumentCreatedBy))
    project = project[0] if project else None
    if project and project.label():
        doc.update({"project": {"uri": str(project.identifier), "name": project.label().toPython()}})
    elif project:
        print("project label missing:", str(project.identifier))    

    #thumbnail for this instrument if found
    thumbnail = get_thumbnail(ins)
    if thumbnail:
        doc.update({"thumbnail": thumbnail})

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
    # Delete and then re-create to instrument index (via PUT request)

    index_url = endpoint + "/dco"

    if rebuild:
        requests.delete(index_url)
        r = requests.put(index_url)
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()

    # push current instrument document mapping

    mapping_url = endpoint + "/dco/instrument/_mapping"
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

    # bulk import new instrument documents
    bulk_import_url = endpoint + "/_bulk"
    r = requests.post(bulk_import_url, data=bulk)
    if r.status_code != requests.codes.ok:
        print(r.url, r.status_code)
        r.raise_for_status()

# generate: startes the ingest process
def generate(threads, sparql):
    pool = multiprocessing.Pool(threads)
    params = [(instrument, sparql) for instrument in get_instruments(endpoint=sparql)]
    return list(itertools.chain.from_iterable(pool.starmap(process_instrument, params)))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=2, help='number of threads to use (default = 8)')
    parser.add_argument('--es', default="http://localhost:9200", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/instrument.json", help="instrument elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    args = parser.parse_args()

    # generate bulk import document for instruments
    records = generate(threads=int(args.threads), sparql=args.sparql)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(records)+'\n')

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)+'\n'
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)


########################################
#
# SOME RUNNING SCRIPTS:
#
# ./bin/elasticsearch
#
# python3 ingest-instruments.py output
# GET dco/instrument/_mapping
# DELETE /dco/instrument/
# DELETE /dco/instrument/_mapping
# curl -XPUT 'localhost:9200/dco/instrument/_mapping?pretty' --data-binary @mappings/dataset.json
# curl -XPOST 'localhost:9200/_bulk' --data-binary @output
#
#########################################
