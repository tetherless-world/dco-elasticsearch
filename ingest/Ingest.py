__author__ = 'Hao'

import multiprocessing
from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
import json
import requests

class Ingest:
    """say something"""

    def __init__(self, object_index, object_type,
                 get_objects_query_location, describe_object_query_location, variable_name_sparql):
        self.object_index = object_index
        self.object_type = object_type
        self.records = []
        self.get_objects_query = Ingest.load_file(get_objects_query_location)
        self.describe_object_query = Ingest.load_file(describe_object_query_location)
        self.variable_name_sparql = variable_name_sparql


    def load_file(filepath):
        with open(filepath) as _file:
            return _file.read().replace('\n', " ")

    # get_metadata: member function to be used in process_object
    def get_metadata(index, type, id):
        return {"index": {"_index": index, "_type": type, "_id": id}}

    def select(endpoint, query):
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results["results"]["bindings"]

    def get_objects(endpoint, get_objects_query, object_type):
        r = Ingest.select(endpoint, get_objects_query)
        return [rs[object_type]["value"] for rs in r]

    # process_object: used by generate
    def process_object(self, object, endpoint, create_object_doc_function):
        # ds = create_dataset_doc(dataset=dataset, endpoint=endpoint) ==>
        # ds = create_object_doc_function(dataset=object, endpoint=endpoint)
        ds = create_object_doc_function(dataset=object, endpoint=endpoint,
                                        describe_object_query=self.describe_object_query,
                                        variable_name_sparql=self.variable_name_sparql)
        if "dcoId" in ds and ds["dcoId"] is not None:
            return [json.dumps(Ingest.get_metadata(self.object_index, self.object_type, (ds["dcoId"]))), json.dumps(ds)]
        else:
            return []


    def generate(self, threads, sparql, create_object_doc_function):
        pool = multiprocessing.Pool(threads)
        # params = [(dataset, sparql) for dataset in get_datasets(endpoint=sparql)] ==>
        params = [(object, sparql, create_object_doc_function)
                  for object in Ingest.get_objects(endpoint=sparql,
                                            get_objects_query=self.get_objects_query,
                                            object_type=self.object_type)]
        # return list(itertools.chain.from_iterable(pool.starmap(process_object_function, params)))
        # self.records = list(itertools.chain.from_iterable(pool.starmap(process_object_function, params)))
        self.records = list(itertools.chain.from_iterable(pool.starmap(self.process_object, params)))

        return self.records


    def publish(self, bulk, endpoint, rebuild, mapping):
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