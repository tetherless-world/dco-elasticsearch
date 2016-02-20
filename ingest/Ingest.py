__author__ = 'Hao'

import multiprocessing
from ingestHelpers import *
import itertools
import json
import requests

class Ingest:
    """Helper class governing an ingest process."""

    def __init__( self ):
        something = None

    def ingest( self ):
        parser = argparse.ArgumentParser()
        parser.add_argument( '--threads', default=4, help='number of threads to use (default = 4)' )
        parser.add_argument( '--es', default="http://localhost:9200", help="elasticsearch service URL" )
        parser.add_argument( '--publish', default=False, action="store_true", help="publish to elasticsearch?" )
        parser.add_argument( '--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?" )
        parser.add_argument( '--mapping', help="elasticsearch mapping document, e.g. mappings/dataset.json" )
        parser.add_argument( '--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint' )
        parser.add_argument( 'out', metavar='OUT', help='elasticsearch bulk ingest file')

        args = parser.parse_args()

        # if a mapping file is specified for the "publish" process later, use the specified mapping file
        self.threads = int( args.threads )
        self.es = args.es
        self.publish = args.publish
        self.rebuild = args.rebuild
        self.endpoint = args.sparql

        if args.mapping:
            self.mapping = args.mapping
        else:
            self.mapping = self.get_mapping()

        # generate bulk import document
        self.generate()

        with open( args.out, "w" ) as bulk_file:
            bulk_file.write( '\n'.join( self.records ) + '\n\n' )

        # publish the results to elasticsearch if "--publish" was specified on the command line
        if args.publish:
            bulk_str = '\n'.join( self.records ) + '\n\n'
            self.publish_to_es( bulk_str )


    def process_entity( self, entity, something ):
        """
        Helper function used by generate() to govern the processing of each subject entity and generate the attributes.
        Note:   The core work here is to creating the JSON-format string describing the entity and is completed by
                member function create_x_doc, which is to be overridden in subclass for different cases.
        :param entity:      the subject entity to be described
        :param endpoint:    SPARQL endpoint
        :return:            An entity entry in JSON
        """
        ds = self.create_document( entity )
        if "dcoId" in ds and ds["dcoId"] is not None:
            return [json.dumps( get_metadata( self.get_index(), self.get_type(), (ds["dcoId"]) ) ), json.dumps(ds)]
        else:
            return []


    def get_entities( self ):
        """
        Helper function used by member function generate(...).
        :return:
            a list of all the entities' uri values
        """
        query = load_file( self.get_list_query_file() )
        r = sparql_select( self.endpoint, query )
        return [rs[self.get_type()]["value"] for rs in r]


    def generate( self ):
        """
        The major method to let an instance of Ingest generate the JSON records and store in self.records.
        :param threads:
        :param sparql: SPARQL endpoint
        :return:
            the output JSON records of this Ingest process.
        """
        pool = multiprocessing.Pool( self.threads )
        sparql = self.endpoint
        #for object in self.get_entities():
        #    json_entity = self.process_entity( object )
        params = [(object,None)
                  for object in self.get_entities()]
        self.records = list(itertools.chain.from_iterable(pool.starmap(self.process_entity, params)))

    def publish_to_es( self, bulk ):
        """
        The majar method to publish the result of the Ingest process.
        :param bulk:        the bulk file containing the ingest result
        :param endpoint:    SPARQL endpoint
        :param rebuild:
        """

        # if configured to rebuild_index
        # Delete and then re-create to publication index (via PUT request)

        index_url = self.es + "/" + self.get_index()

        if self.rebuild:
            requests.delete( index_url )
            r = requests.put( index_url )
            if r.status_code != requests.codes.ok:
                print(r.url, r.status_code)
                r.raise_for_status()

        # push current mapping

        mapping_url = index_url + "/" + self.get_type() + "/_mapping"
        with open( self.mapping ) as mapping_file:
            r = requests.put( mapping_url, data=mapping_file )
            if r.status_code != requests.codes.ok:

                # new mapping may be incompatible with previous
                # delete current mapping and re-push

                requests.delete(mapping_url)
                r = requests.put( mapping_url, data=mapping_file )
                if r.status_code != requests.codes.ok:
                    print( r.url, r.status_code )
                    r.raise_for_status()

        # bulk import new publication documents
        bulk_import_url = self.es + "/_bulk"
        r = requests.post( bulk_import_url, data=bulk )
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()


    # describe_entity: helper function for create_document
    def describe_entity( self, entity ):
        query = load_file( self.get_describe_query_file() )
        query = query.replace( self.get_subject_name(), "<" + entity + ">" )
        return sparql_describe( self.endpoint, query )

