__author__ = 'Hao'

import multiprocessing
from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
import json
import requests

class Ingest:
    """Helper class governing an ingest process."""

    def __init__(self, object_index, object_type,
                 get_objects_query_location, describe_object_query_location, variable_name_sparql):
        """
        Constructor method of the Ingest class.
        :param object_index:                    object index
        :param object_type:                     object type
        :param get_objects_query_location:      the location of the .rq file to list all the objects
        :param describe_object_query_location:  the location of the .rq file to describe all the objects
        :param variable_name_sparql:            the variable name representing the object in the .rq files,
                                                    e.g. "?dataset"
        :return:                                an instance of Ingest.
        """
        self.object_index = object_index
        self.object_type = object_type
        self.records = []
        self.get_objects_query = Ingest.load_file(get_objects_query_location)
        self.describe_object_query = Ingest.load_file(describe_object_query_location)
        self.variable_name_sparql = variable_name_sparql


    def load_file(filepath):
        """
        Helper function mainly used by the __init__ to load the .rq files.
        :param filepath:    file path
        :return:            file content in string format
        """
        with open(filepath) as _file:
            return _file.read().replace('\n', " ")


    def get_metadata(index, type, id):
        """
        Helper function to create the JSON string of the metadata of an object.
        :param index:   object index
        :param type:    object type
        :param id:      unique identifier of the object
        :return:
            a JSON string representing the metadata information of the object,
                            e.g. {"index": {"_id": "http://...", "_type": "dataset", "_index": "dco"}}
        """
        return {"index": {"_index": index, "_type": type, "_id": id}}


    def process_object(self, object, endpoint, create_object_doc_function):
        """
        Helper function used by generate() to process each object and generate the attribute
        :param object:      the object to be described
        :param endpoint:    SPARQL endpoint
        :param create_object_doc_function:
            Externally defined, case-varying function to create the JSON document. The function name is passed in
            originally through Ingest.generate(...).
        :return:            An object entry in JSON
        """
        ds = create_object_doc_function(dataset=object, endpoint=endpoint,
                                        describe_object_query=self.describe_object_query,
                                        variable_name_sparql=self.variable_name_sparql)
        if "dcoId" in ds and ds["dcoId"] is not None:
            return [json.dumps(Ingest.get_metadata(self.object_index, self.object_type, (ds["dcoId"]))), json.dumps(ds)]
        else:
            return []


    def select(endpoint, query):
        """
        Helper function used by get_objects
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


    def get_objects(endpoint, get_objects_query, object_type):
        """
        Helper function used by Ingest.generate(...).
        :param endpoint:            SPARQL endpoint
        :param get_objects_query:   the SPARQL query to get the list of objects
        :param object_type:         object type
        :return:
            a list of all the objects' uri values
        """
        r = Ingest.select(endpoint, get_objects_query)
        return [rs[object_type]["value"] for rs in r]


    def generate(self, threads, sparql, create_object_doc_function):
        """
        The major method to let an instance of Ingest generate the JSON records.
        :param threads:
        :param sparql: SPARQL endpoint
        :param create_object_doc_function:
        :return:
            the output JSON records of this Ingest process.
        """
        pool = multiprocessing.Pool(threads)
        params = [(object, sparql, create_object_doc_function)
                  for object in Ingest.get_objects(endpoint=sparql,
                                            get_objects_query=self.get_objects_query,
                                            object_type=self.object_type)]
        self.records = list(itertools.chain.from_iterable(pool.starmap(self.process_object, params)))
        return self.records


    def publish(self, bulk, endpoint, rebuild, mapping):
        """
        The majar method to publish the result of the Ingest process.
        :param bulk:        the bulk file containing the ingest result
        :param endpoint:    SPARQL endpoint
        :param rebuild:
        :param mapping:     the mapping file
        """
        # if configured to rebuild_index
        # Delete and then re-create to publication index (via PUT request)

        index_url = endpoint + "/" + self.object_index

        if rebuild:
            requests.delete(index_url)
            r = requests.put(index_url)
            if r.status_code != requests.codes.ok:
                print(r.url, r.status_code)
                r.raise_for_status()

        # push current publication document mapping

        mapping_url = index_url + "/" + self.object_type + "/_mapping"
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