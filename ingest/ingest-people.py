__author__ = 'szednik'

from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Namespace, RDF
import json
import requests
import multiprocessing
from itertools import chain
import functools
import argparse


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

# standard filters
non_empty_str = lambda s: True if s else False
has_label = lambda o: True if o.label() else False


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


def get_dcoid(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(DCO.hasDcoId)) \
        .orElse(lambda: person.graph.subjects(DCO.dcoIdFor, person)) \
        .map(lambda i: i.identifier).one().value


def get_orcid(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(VIVO.orcidId)) \
        .map(lambda o: o.identifier) \
        .map(lambda o: o[o.rfind('/') + 1:]).one().value


def get_most_specific_type(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(VITRO.mostSpecificType)) \
        .map(lambda t: t.label()) \
        .filter(non_empty_str) \
        .one().value


def get_network_id(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(NET_ID.networkId)) \
        .filter(non_empty_str) \
        .one().value


def get_given_name(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(OBO.ARG_2000028)) \
        .flatmap(lambda v: v.objects(VCARD.hasName)) \
        .flatmap(lambda n: n.objects(VCARD.givenName)) \
        .filter(non_empty_str) \
        .one().value


def get_family_name(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(OBO.ARG_2000028)) \
        .flatmap(lambda v: v.objects(VCARD.hasName)) \
        .flatmap(lambda n: n.objects(VCARD.familyName)) \
        .filter(non_empty_str) \
        .one().value


def get_email(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(OBO.ARG_2000028)) \
        .flatmap(lambda v: v.objects(VCARD.hasEmail)) \
        .filter(lambda f: has_type(f, VCARD.Work)) \
        .flatmap(lambda e: e.objects(VCARD.email)) \
        .filter(non_empty_str) \
        .one().value


def get_research_areas(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(VIVO.hasResearchArea)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).list()


def get_organizations(person):
    orgs = []

    orgroles = Maybe.of(person).stream() \
        .flatmap(lambda per: per.objects(VIVO.relatedBy)) \
        .filter(lambda related: has_type(related, VIVO.Position)).list()

    for orgrole in orgroles:
        org = Maybe.of(orgrole).stream() \
            .flatmap(lambda r: r.objects(VIVO.relates)) \
            .filter(lambda o: has_type(o, FOAF.Organization)) \
            .filter(has_label) \
            .map(lambda o: {"uri": str(o.identifier), "name": str(o.label())}) \
            .one().value

        if org:
            orgs.append({"orgrole": str(orgrole.label()), "organization": org})

    return orgs


def get_teams(person):
    teams = []

    teamroles = Maybe.of(person).stream() \
        .flatmap(lambda per: per.objects(OBO.RO_0000053)) \
        .filter(lambda related: has_type(related, VIVO.MemberRole)).list()

    for teamrole in teamroles:
        team = Maybe.of(teamrole).stream() \
            .flatmap(lambda r: r.objects(VIVO.roleContributesTo)) \
            .filter(lambda o: has_type(o, DCO.Team)) \
            .filter(has_label) \
            .map(lambda o: {"uri": str(o.identifier), "name": str(o.label())}) \
            .one().value

        if team:
            teams.append({"teamrole": str(teamrole.label()), "team": team})

    return teams


def get_dco_communities(person):
    comms = []

    commroles = Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(OBO.RO_0000053)) \
        .filter(lambda related: has_type(related, VIVO.MemberRole)).list()

    for commrole in commroles:
        comm = Maybe.of(commrole).stream() \
            .flatmap(lambda r: r.objects(VIVO.roleContributesTo)) \
            .filter(lambda o: has_type(o, DCO.ResearchCommunity)) \
            .filter(has_label) \
            .map(lambda o: {"uri": str(o.identifier), "name": str(o.label())}) \
            .one().value

        if comm:
            comms.append({"commrole": str(commrole.label()), "community": comm})

    return comms


def get_home_country(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(DCO.homeCountry)) \
        .filter(has_label) \
        .map(lambda r: {"uri": str(r.identifier), "name": str(r.label())}).one().value


def get_thumbnail(person):
    return Maybe.of(person).stream() \
        .flatmap(lambda p: p.objects(VITRO_PUB.mainImage)) \
        .flatmap(lambda i: i.objects(VITRO_PUB.thumbnailImage)) \
        .flatmap(lambda t: t.objects(VITRO_PUB.downloadLocation)) \
        .map(lambda t: t.identifier) \
        .one().value


def create_person_doc(person, endpoint):
    graph = describe_person(endpoint=endpoint, person=person)

    per = graph.resource(person)

    try:
        name = per.label()
    except AttributeError:
        print("missing name:", person)
        return {}

    dcoid = get_dcoid(per)
    doc = {"uri": person, "name": name, "dcoId": dcoid}

    orcid = get_orcid(per)
    if orcid:
        doc.update({"orcid": orcid})

    network_id = get_network_id(per)
    if network_id:
        doc.update({"network_id": network_id, "isDcoMember": True})
    else:
        doc.update({"isDcoMember": False})

    most_specific_type = get_most_specific_type(per)
    if most_specific_type:
        doc.update({"mostSpecificType": most_specific_type})

    given_name = get_given_name(per)
    if given_name:
        doc.update({"givenName": given_name})

    family_name = get_family_name(per)
    if family_name:
        doc.update({"familyName": family_name})

    email = get_email(per)
    if email:
        doc.update({"email": email})

    research_areas = get_research_areas(per)
    if research_areas:
        doc.update({"researchArea": research_areas})

    home_country = get_home_country(per)
    if home_country:
        doc.update({"homeCountry": home_country})

    organizations = get_organizations(per)
    if organizations:
        doc.update({"organizations": organizations})

    teams = get_teams(per)
    if teams:
        doc.update({"teams": teams})

    dco_communities = get_dco_communities(per)
    if dco_communities:
        doc.update({"dcoCommunities": dco_communities})

    thumbnail = get_thumbnail(per)
    if thumbnail:
        doc.update({"thumbnail": thumbnail})

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
    return list(chain.from_iterable(pool.starmap(process_person, params)))


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
        bulk_file.write('\n'.join(records)+'\n')

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)+'\n'
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)
