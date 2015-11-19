__author__ = 'Hao'

import multiprocessing
from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
import json
import requests

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


class Ingest:
    """Helper class governing an ingest process."""

    # MAPPING: class variable for the relative location of the mapping file for publishing;
    #          its default value varies for different subclasses
    MAPPING = "mappings/x.json"

    def __init__(self, elasticsearch_index, elasticsearch_type,
                 get_objects_query_location, describe_object_query_location, variable_name_sparql):
        """
        __init__: constructor method of the Ingest class.
        Member variables w/ value taken in by constructor:
            self.elasticsearch_index:                   elasticsearch index
            self.elasticsearch_type:                    elasticsearch type
            self.get_objects_query_location:            location of the .rq file to list all the subject entities
            self.describe_object_query_location:        location of the .rq file to describe an entity
            self.variable_name_sparql:                  variable name of the subject entity in queries, e.g. "?dataset"
        Member variables w/ default value given (may subject to later changes):
            self.records                the output data of the ingest process
            self.mapping                location of the mapping file w/ default value set to class attribute MAPPING
        """
        self.elasticsearch_index = elasticsearch_index
        self.elasticsearch_type = elasticsearch_type
        self.get_objects_query = Ingest.load_file(get_objects_query_location)
        self.describe_object_query = Ingest.load_file(describe_object_query_location)
        self.variable_name_sparql = variable_name_sparql
        self.records = []
        self.mapping = self.MAPPING


    def setMapping(self, m): self.mapping = m

    def getMapping(self): return self.mapping

    def load_file(filepath):
        """
        Helper function to load the .rq files and return a String object w/ replacing '\n' by ' '.
        :param filepath:    file path
        :return:            file content in string format
        """
        with open(filepath) as _file:
            return _file.read().replace('\n', " ")


    def get_metadata(index, type, id):
        """
        Helper function to create the JSON string of the metadata of an entity.
        :param index:   elastic search index
        :param type:    elastic search type
        :param id:      unique identifier of the entity
        :return:
            a JSON-format string representing the metadata information of the object,
                e.g. {"index": {"_id": "http://...", "_type": "dataset", "_index": "dco"}}
        """
        return {"index": {"_index": index, "_type": type, "_id": id}}


    def process_entity(self, entity, endpoint):
        """
        Helper function used by generate() to govern the processing of each subject entity and generate the attributes.
        Note:   The core work here is to creating the JSON-format string describing the entity and is completed by
                member function create_x_doc, which is to be overridden in subclass for different cases.
        :param entity:      the subject entity to be described
        :param endpoint:    SPARQL endpoint
        :return:            An entity entry in JSON
        """
        ds = self.create_x_doc(x=entity, endpoint=endpoint,
                                        describe_object_query=self.describe_object_query,
                                        variable_name_sparql=self.variable_name_sparql)
        if "dcoId" in ds and ds["dcoId"] is not None:
            return [json.dumps(Ingest.get_metadata(self.elasticsearch_index, self.elasticsearch_type, (ds["dcoId"]))), json.dumps(ds)]
        else:
            return []


    def select(endpoint, query):
        """
        Helper function used by get_entities
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


    def get_entities(endpoint, get_objects_query, elasticsearch_type):
        """
        Helper function used by member function generate(...).
        :param endpoint:                    SPARQL endpoint
        :param get_objects_query:           SPARQL query to get the list of objects
        :param elasticsearch_type:          elasticsearch type
        :return:
            a list of all the entities' uri values
        """
        r = Ingest.select(endpoint, get_objects_query)
        return [rs[elasticsearch_type]["value"] for rs in r]


    def generate(self, threads, sparql):
        """
        The major method to let an instance of Ingest generate the JSON records and store in self.records.
        :param threads:
        :param sparql: SPARQL endpoint
        :return:
            the output JSON records of this Ingest process.
        """
        pool = multiprocessing.Pool(threads)
        params = [(object, sparql)
                  for object in Ingest.get_entities(endpoint=sparql,
                                            get_objects_query=self.get_objects_query,
                                            elasticsearch_type=self.elasticsearch_type)]
        self.records = list(itertools.chain.from_iterable(pool.starmap(self.process_entity, params)))

        return self.records


    def publish(self, bulk, endpoint, rebuild):
        """
        The majar method to publish the result of the Ingest process.
        :param bulk:        the bulk file containing the ingest result
        :param endpoint:    SPARQL endpoint
        :param rebuild:
        """

        # if configured to rebuild_index
        # Delete and then re-create to publication index (via PUT request)

        index_url = endpoint + "/" + self.elasticsearch_index

        if rebuild:
            requests.delete(index_url)
            r = requests.put(index_url)
            if r.status_code != requests.codes.ok:
                print(r.url, r.status_code)
                r.raise_for_status()

        # push current publication document mapping

        mapping_url = index_url + "/" + self.elasticsearch_type + "/_mapping"
        with open(self.mapping) as mapping_file:
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


    # describe: helper function for describe_entity
    def describe(self, endpoint, query):
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        try:
            return sparql.query().convert()
        except RuntimeWarning:
            pass


    # describe_entity: helper function for create_x_doc
    def describe_entity(self, endpoint, entity, describe_object_query, variable_name_sparql):
        q = describe_object_query.replace(variable_name_sparql, "<" + entity + ">")
        return self.describe(endpoint, q)


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

        return doc