__author__ = 'szednik'

from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Namespace, RDF
import json
import requests
import multiprocessing
import itertools
import argparse


def load_file(filepath):
    with open(filepath) as _file:
        return _file.read().replace('\n', " ")


PROV = Namespace("http://www.w3.org/ns/prov#")
BIBO = Namespace("http://purl.org/ontology/bibo/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
VITRO_PUB = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/public#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
DCO = Namespace("http://info.deepcarbon.net/schema#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
NET_ID = Namespace("http://vivo.mydomain.edu/ns#")

get_people_query = load_file("queries/listPeople.rq")
describe_person_query = load_file("queries/describePerson.rq")


def get_metadata(id):
    return {"index": {"_index": "dco", "_type": "person", "_id": id}}


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


def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False


def get_people(endpoint):
    r = select(endpoint, get_people_query)
    return [rs["person"]["value"] for rs in r]


def describe_person(endpoint, person):
    q = describe_person_query.replace("?person", "<" + person + ">")
    return describe(endpoint, q)


def create_person_doc(person, endpoint):
    graph = describe_person(endpoint=endpoint, person=person)

    per = graph.resource(person)

    try:
        name = per.label().toPython()
    except AttributeError:
        print("missing name:", person)
        return {}

    dco_id = list(per.objects(DCO.hasDcoId))
    dco_id = list(graph.subjects(DCO.dcoIdFor, per)) if dco_id is None else dco_id
    dco_id = str(dco_id[0].identifier) if dco_id else None

    doc = {"uri": person, "name": name, "dcoId": dco_id}

    orcid = list(per.objects(VIVO.orcidId))
    orcid = str(orcid[0].identifier) if orcid else None
    if orcid:
        orcid = orcid[orcid.rfind('/') + 1:]
        doc.update({"orcid": orcid})

    network_id = list(per.objects(NET_ID.networkId))
    network_id = network_id[0].toPython() if network_id else None
    if network_id:
        doc.update({"network_id": network_id})

    is_dco_member = True if network_id is not None else False
    doc.update({"isDcoMember": is_dco_member})

    most_specific_type = list(per.objects(VITRO.mostSpecificType))
    most_specific_type = most_specific_type[0].label().toPython() \
        if most_specific_type and most_specific_type[0].label() \
        else None
    if most_specific_type:
        doc.update({"mostSpecificType": most_specific_type})

    vcard = list(per.objects(OBO.ARG_2000028))
    vcard = vcard[0] if vcard else None

    vcard_name = list(vcard.objects(VCARD.hasName)) if vcard else None
    vcard_name = vcard_name[0] if vcard_name else None

    given_name = list(vcard_name.objects(VCARD.givenName)) if vcard_name else None
    if given_name:
        doc.update({"givenName": given_name[0].toPython()})

    family_name = list(vcard_name.objects(VCARD.familyName)) if vcard_name else None
    if family_name:
        doc.update({"familyName": family_name[0].toPython()})

    if vcard:
        vcard_email = [x for x in vcard.objects(VCARD.hasEmail) if has_type(x, VCARD.Work)]
        vcard_email = vcard_email[0] if vcard_email else None
        email = list(vcard_email.objects(VCARD.email)) if vcard_email else None
        if email:
            doc.update({"email": email[0].toPython()})

    research_areas = [{"uri": str(research_area.identifier), "name": research_area.label().toPython()} for research_area in per.objects(VIVO.hasResearchArea) if research_area.label()]
    if research_areas:
        doc.update({"researchArea": research_areas})

    orgs = [{"uri": str(org.identifier), "name": org.label().toPython()} for org in per.objects(DCO.inOrganization) if org.label()]
    if orgs:
        doc.update({"organization": orgs})

    portal_groups = [{"uri": str(pg.identifier), "name": pg.label().toPython()} for pg in per.objects(DCO.associatedDCOPortalGroup) if pg.label()]
    if portal_groups:
        doc.update({"portalGroups": portal_groups})

    dco_communities = [{"uri": str(comm.identifier), "name": comm.label().toPython()} for comm in per.objects(DCO.associatedDCOCommunity) if comm.label()]
    if dco_communities:
        doc.update({"dcoCommunities": dco_communities})

    main_image = list(per.objects(VITRO_PUB.mainImage))
    main_image = main_image[0] if main_image else None

    thumb_image = list(main_image.objects(VITRO_PUB.thumbnailImage)) if main_image else None
    thumb_image = thumb_image[0] if thumb_image else None

    thumb_image_download = list(thumb_image.objects(VITRO_PUB.downloadLocation)) if thumb_image else None
    thumb_image_download = thumb_image_download[0] if thumb_image_download else None

    if thumb_image_download:
        doc.update({"thumbnail": thumb_image_download.identifier})

    positions = [x for x in per.objects(VIVO.relatedBy) if has_type(x, VIVO.Position)]
    affiliations = []
    for position in positions:
        org = [x for x in position.objects(VIVO.relates) if has_type(x, FOAF.Organization)][0]
        affiliations.append({"position": position.label().toPython(), "org": {"uri": str(org.identifier), "name": org.label().toPython()}})

    if affiliations:
        doc.update({"affiliations": affiliations})

    return doc


def process_person(person, endpoint):
    per = create_person_doc(person=person, endpoint=endpoint)
    es_id = per["dcoId"] if "dcoId" in per and per["dcoId"] is not None else per["uri"]
    es_id = get_id(es_id)
    return [json.dumps(get_metadata(es_id)), json.dumps(per)]


def publish(bulk, endpoint, rebuild, mapping):
    # if configured to rebuild_index
    # Delete and then re-create to publication index (via PUT request)

    index_url = endpoint + "/dco"

    if rebuild:
        requests.delete(index_url)
        r = requests.put(index_url)
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()

    # push current publication document mapping

    mapping_url = endpoint + "/dco/person/_mapping"
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
    params = [(person, sparql) for person in get_people(endpoint=sparql)]
    return list(itertools.chain.from_iterable(pool.starmap(process_person, params)))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=8, help='number of threads to use (default = 8)')
    parser.add_argument('--es', default="http://data.deepcarbon.net/es", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/person.json", help="publication elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    args = parser.parse_args()

    # generate bulk import document for publications
    records = generate(threads=int(args.threads), sparql=args.sparql)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)
