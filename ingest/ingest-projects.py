__author__ = 'am-e'

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


get_projects_query = load_file("queries/listProjects.rq")
describe_project_query = load_file("queries/describeProject.rq")

PROV = Namespace("http://www.w3.org/ns/prov#")
BIBO = Namespace("http://purl.org/ontology/bibo/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/0.7#")
VITRO_PUB = Namespace("http://vitro.mannlib.cornell.edu/ns/vitro/public#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
DCO = Namespace("http://info.deepcarbon.net/schema#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")


def get_metadata(id):
    return {"index": {"_index": "dco", "_type": "project", "_id": id}}


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


def get_projects(endpoint):
    r = select(endpoint, get_projects_query)
    return [rs["project"]["value"] for rs in r]


def process_project(project, endpoint):
    prj = create_project_doc(project=project, endpoint=endpoint)
    if "dcoId" in prj and prj["dcoId"] is not None:
        return [json.dumps(get_metadata(get_id(prj["dcoId"]))), json.dumps(prj)]
    else:
        return []


def describe_project(endpoint, project):
    q = describe_project_query.replace("?project", "<" + project + ">")
    return describe(endpoint, q)

def get_thumbnail(project):
    return Maybe.of(project).stream() \
        .flatmap(lambda p: p.objects(VITRO_PUB.mainImage)) \
        .flatmap(lambda i: i.objects(VITRO_PUB.thumbnailImage)) \
        .flatmap(lambda t: t.objects(VITRO_PUB.downloadLocation)) \
        .map(lambda t: t.identifier) \
        .one().value

def create_project_doc(project, endpoint):
    graph = describe_project(endpoint=endpoint, project=project)

    prj = graph.resource(project)

    try:
        title = prj.label().toPython()
    except AttributeError:
        print("missing title:", project)
        return {}

    dco_id = list(prj.objects(DCO.hasDcoId))
    dco_id = str(dco_id[0].identifier) if dco_id else None

    doc = {"uri": project, "title": title, "dcoId": dco_id}

    most_specific_type = list(prj.objects(VITRO.mostSpecificType))
    most_specific_type = most_specific_type[0].label().toPython() \
        if most_specific_type and most_specific_type[0].label() \
        else None
    if most_specific_type:
        doc.update({"mostSpecificType": most_specific_type})

    submitted_by = list(prj.objects(DCO.submittedBy))
    submitted_by = submitted_by[0] if submitted_by else None
    if submitted_by and submitted_by.label():
        doc.update({"submittedBy": {"uri": str(submitted_by.identifier), "name": submitted_by.label().toPython()}})
    elif submitted_by:
        print("submitted-by label missing:", str(submitted_by.identifier))

    leader = list(prj.objects(DCO.fieldworkLeader))
    leader = leader[0] if leader else None
    if leader and leader.label():
        doc.update({"leader": {"uri": str(leader.identifier), "name": leader.label().toPython()}})
    elif leader:
        print("leader label missing:", str(leader.identifier))

    date_time_interval = list(prj.objects(VIVO.dateTimeInterval))
    date_time_interval = date_time_interval[0] if date_time_interval else None

    if date_time_interval is not None:

        start = list(date_time_interval.objects(VIVO.start))
        start = start[0] if start else None

        end = list(date_time_interval.objects(VIVO.end))
        end = end[0] if end else None

        if start is not None:
            start_date_time = list(start.objects(VIVO.dateTime))
            start_date_time = start_date_time[0] if start_date_time else None

        if end is not None:
            end_date_time = list(end.objects(VIVO.dateTime))
            end_date_time = end_date_time[0] if end_date_time else None


        if start is not None and end is not None:
            doc.update({"dateTimeInterval": {"uri": str(date_time_interval.identifier), "startDate": str(start_date_time)[:10], "startYear": str(start_date_time)[:4], "endDate": str(end_date_time)[:10], "endYear": str(end_date_time)[:4]}})

        if start is not None and end is None:
            doc.update({"dateTimeInterval": {"uri": str(date_time_interval.identifier), "startDate": str(start_date_time)[:10], "startYear": str(start_date_time)[:4]}})

        if end is not None and start is None:
            doc.update({"dateTimeInterval": {"uri": str(date_time_interval.identifier), "endDate": str(end_date_time)[:10], "endYear": str(end_date_time)[:4]}})


    associatedCommunities = []
    communities = [faux for faux in prj.objects(DCO.associatedDCOCommunity) if has_type(faux, DCO.ResearchCommunity)]

    if communities:
        for community in communities:
            #print(community)
            name = community.label().toPython() if community else None

            obj = {"uri": str(community.identifier), "name": name}

            associatedCommunities.append(obj)

    doc.update({"dcoCommunities": associatedCommunities})

    associatedPortalGroups = []
    portalGroups = [faux for faux in prj.objects(DCO.associatedDCOPortalGroup) if has_type(faux, DCO.PortalGroup)]

    if portalGroups:
        for portalGroup in portalGroups:

            name = portalGroup.label().toPython() if portalGroup else None

            obj = {"uri": str(portalGroup.identifier), "name": name}

            associatedPortalGroups.append(obj)

    doc.update({"dcoPortalGroups": associatedPortalGroups})


    participants = []
    roles = [faux for faux in prj.objects(OBO.BFO_0000055) if has_type(faux, VIVO.Role)]

    if roles:
        for role in roles:

            participant = [person for person in role.objects(OBO.RO_0000052) if has_type(person, FOAF.Person)][0]
            name = participant.label().toPython() if participant else None

            obj = {"uri": str(participant.identifier), "name": name}

            research_areas = [research_area.label().toPython() for research_area in participant.objects(VIVO.hasResearchArea) if research_area.label()]

            if research_areas:
                obj.update({"researchArea": research_areas})

            org = list(participant.objects(DCO.inOrganization))
            org = org[0] if org else None
            if org and org.label():
                obj.update({"organization": {"uri": str(org.identifier), "name": org.label().toPython()}})

            participants.append(obj)

    roles = [faux for faux in prj.objects(VIVO.contributingRole) if has_type(faux, VIVO.Role)]

    if roles:
        for role in roles:

            participant = [person for person in role.objects(OBO.RO_0000052) if has_type(person, FOAF.Person)][0]
            name = participant.label().toPython() if participant else None

            obj = {"uri": str(participant.identifier), "name": name}

            research_areas = [research_area.label().toPython() for research_area in participant.objects(VIVO.hasResearchArea) if research_area.label()]

            if research_areas:
                obj.update({"researchArea": research_areas})

            org = list(participant.objects(DCO.inOrganization))
            org = org[0] if org else None
            if org and org.label():
                obj.update({"organization": {"uri": str(org.identifier), "name": org.label().toPython()}})

            participants.append(obj)

    doc.update({"participants": participants})

    reporting_years = []
    project_updates = [faux for faux in prj.objects(DCO.hasProjectUpdate) if has_type(faux, DCO.ProjectUpdate)]

    if project_updates:
        for project_update in project_updates:

            reporting_year = list(project_update.objects(DCO.forReportingYear))
            reporting_year = reporting_year[0] if reporting_year else None


            obj = {"uri": str(reporting_year.identifier), "year": reporting_year.label().toPython()}

            reporting_years.append(obj)

    doc.update({"reportingYear": reporting_years})

    grants = []
    fundingVehicles = [faux for faux in prj.objects(VIVO.hasFundingVehicle) if has_type(faux, VIVO.Grant)]

    if fundingVehicles:
        for fundingVehicle in fundingVehicles:

            name = fundingVehicle.label().toPython() if fundingVehicle else None

            obj = {"uri": str(fundingVehicle.identifier), "name": name}

            grants.append(obj)

    doc.update({"grants": grants})


    thumbnail = get_thumbnail(prj)
    if thumbnail:
        doc.update({"thumbnail": thumbnail})

    return doc


def has_type(resource, type):
    for rtype in resource.objects(RDF.type):
        if str(rtype.identifier) == str(type):
            return True
    return False


def publish(bulk, endpoint, rebuild, mapping):
    # if configured to rebuild_index
    # Delete and then re-create to project index (via PUT request)

    index_url = endpoint + "/dco"

    if rebuild:
        requests.delete(index_url)
        r = requests.put(index_url)
        if r.status_code != requests.codes.ok:
            print(r.url, r.status_code)
            r.raise_for_status()

    # push current project document mapping

    mapping_url = endpoint + "/dco/project/_mapping"
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

    # bulk import new project documents
    bulk_import_url = endpoint + "/_bulk"
    r = requests.post(bulk_import_url, data=bulk)
    if r.status_code != requests.codes.ok:
        print(r.url, r.status_code)
        r.raise_for_status()


def generate(threads, sparql):
    pool = multiprocessing.Pool(threads)
    params = [(project, sparql) for project in get_projects(endpoint=sparql)]
    return list(itertools.chain.from_iterable(pool.starmap(process_project, params)))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', default=2, help='number of threads to use (default = 8)')
    parser.add_argument('--es', default="http://data.deepcarbon.net/es", help="elasticsearch service URL")
    parser.add_argument('--publish', default=False, action="store_true", help="publish to elasticsearch?")
    parser.add_argument('--rebuild', default=False, action="store_true", help="rebuild elasticsearch index?")
    parser.add_argument('--mapping', default="mappings/project.json", help="project elasticsearch mapping document")
    parser.add_argument('--sparql', default='http://deepcarbon.tw.rpi.edu:3030/VIVO/query', help='sparql endpoint')
    parser.add_argument('out', metavar='OUT', help='elasticsearch bulk ingest file')

    args = parser.parse_args()

    # generate bulk import document for projects
    records = generate(threads=int(args.threads), sparql=args.sparql)

    # save generated bulk import file so it can be backed up or reviewed if there are publish errors
    with open(args.out, "w") as bulk_file:
        bulk_file.write('\n'.join(records))

    # publish the results to elasticsearch if "--publish" was specified on the command line
    if args.publish:
        bulk_str = '\n'.join(records)
        publish(bulk=bulk_str, endpoint=args.es, rebuild=args.rebuild, mapping=args.mapping)
