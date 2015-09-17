__author__ = 'szednik'

from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Namespace, RDF
import json
import requests
import multiprocessing
import itertools
from itertools import chain
import functools
import argparse
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


get_datasets_query = load_file("queries/listDatasets.rq")
describe_dataset_query = load_file("queries/describeDataset.rq")

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

_index = "dco"
_type = "dataset"


def get_metadata(id):
    return {"index": {"_index": _index, "_type": _type, "_id": id}}


def get_id(dco_id):
    return dco_id[dco_id.rfind('/') + 1:]


def select(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


def describe(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    try:
        return sparql.query().convert()
    except RuntimeWarning:
        pass


def get_datasets(endpoint):
    r = select(endpoint, get_datasets_query)
    return [rs["dataset"]["value"] for rs in r]


def process_dataset(dataset, endpoint):
    ds = create_dataset_doc(dataset=dataset, endpoint=endpoint)
    if "dcoId" in ds and ds["dcoId"] is not None:
        return [json.dumps(get_metadata(get_id(ds["dcoId"]))), json.dumps(ds)]
    else:
        return []


def describe_dataset(endpoint, dataset):
    q = describe_dataset_query.replace("?dataset", "<" + dataset + ">")
    return describe(endpoint, q)


def get_dco_communities(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOCommunity)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_portal_groups(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(DCO.associatedDCOPortalGroup)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_data_types(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(DCO.hasDataType)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_cites(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(BIBO.cites)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()

def get_projects(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(DCO.isDatasetOf)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()


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

    is_dco_publication = list(ds.objects(DCO.isDCOPublication))
    is_dco_publication = True if is_dco_publication and is_dco_publication[0].toPython() == "YES" else False

    doc = {"uri": dataset, "title": title, "dcoId": dco_id, "isDcoPublication": is_dco_publication}

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

    dco_community = list(ds.objects(DCO.associatedDCOCommunity))
    dco_community = dco_community[0] if dco_community else None
    if dco_community and dco_community.label():
        doc.update({"community": {"uri": str(dco_community.identifier), "name": dco_community.label().toPython()}})
    elif dco_community:
        print("community label missing:", str(dco_community.identifier))

    # dco_communities
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
    authors = []
    authorships = [faux for faux in ds.objects(VIVO.relatedBy) if has_type(faux, VIVO.Authorship)]
    for authorship in authorships:

        author = [person for person in authorship.objects(VIVO.relates) if has_type(person, FOAF.Person)][0]
        name = author.label().toPython() if author else None

        obj = {"uri": str(author.identifier), "name": name}

        rank = list(authorship.objects(VIVO.rank))
        rank = rank[0].toPython() if rank else None
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

    doc.update({"authors": authors})

    # ============================================================
    # distributions
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

    # try:
    #     authors = sorted(authors, key=lambda a: a["rank"]) if len(authors) > 1 else authors
    # except KeyError:
    #     print("missing rank for one or more authors of:", dataset)

    doc.update({"distributions": distributions})

    # ===========================================

    return doc


def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False


def publish(bulk, endpoint, rebuild, mapping):
    # if configured to rebuild_index
    # Delete and then re-create to publication index (via PUT request)

    index_url = endpoint + "/" + _index

    if rebuild:
        requests.delete(index_url)
        r = requests.put(index_url)
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()

    # push current publication document mapping

    mapping_url = index_url + "/" + _type + "/_mapping"
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


def generate(threads, sparql):
    pool = multiprocessing.Pool(threads)
    params = [(dataset, sparql) for dataset in get_datasets(endpoint=sparql)]
    return list(itertools.chain.from_iterable(pool.starmap(process_dataset, params)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=1, help='number of threads to use (default = 8)')
    # parser.add_argument('--es', default="http://localhost:9200", help="elasticsearch service URL")
    parser.add_argument('--es', default="https://dcotest.tw.rpi.edu/search", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/dataset.json", help="dataset elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    # parser.add_argument('--sparql', default='http://udco.tw.rpi.edu/fuseki/vivo/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    args = parser.parse_args()

    # generate bulk import document for datasets
    records = generate(threads=int(args.threads), sparql=args.sparql)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)

    # SOME RUNNING SCRIPTS:
    # python3 ingest-datasets.py output
    # GET dco/dataset/_mapping
    # DELETE /dco/dataset/_mapping
    # curl -XPUT 'localhost:9200/dco/dataset/_mapping?pretty' --data-binary @mappings/dataset.json
    # curl -XPOST 'localhost:9200/_bulk' --data-binary @output
