__author__ = 'szednik'

from SPARQLWrapper import SPARQLWrapper, JSON
import json


def load_file(filepath):
    with open(filepath) as _file:
        return _file.read().replace('\n', " ")


get_publication_authors_query = load_file("queries/getPublicationAuthors.rq")
get_publication_info_query = load_file("queries/getPublicationInfo.rq")
get_publications_query = load_file("queries/listPublications.rq")

vivo_endpoint = "http://deepcarbon.tw.rpi.edu:3030/VIVO/query"
sparql = SPARQLWrapper(vivo_endpoint)


def get_metadata(id):
    return {"index": {"_index": "dco", "_type": "publication", "_id": id}}


def get_id(dco_id):
    return dco_id[dco_id.rfind('/') + 1:]


def query(query):
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


def get_publications():
    r = query(get_publications_query)
    return [rs["publication"]["value"] for rs in r]


def get_publication_info(publication):
    _query = get_publication_info_query.replace("{publication}", "<" + publication + ">")
    r = query(_query)

    info = {}

    for rs in r:
        community = rs["community"]["value"].strip() if 'community' in rs else None
        community_name = rs["communityName"]["value"].strip() if 'communityName' in rs else None
        dco_id = rs["dcoId"]["value"].strip() if "dcoId" in rs else None
        title = rs["title"]["value"].strip()
        venue = rs["venue"]["value"].strip() if "venue" in rs else None
        venue_name = rs["venueName"]["value"].strip() if "venueName" in rs else None
        abstract = rs["abstract"]["value"].strip() if "abstract" in rs else None
        publication_year = rs["publicationYear"]["value"] if "publicationYear" in rs else None
        subject_area = rs["subjectArea"]["value"] if "subjectArea" in rs else None
        subject_area_name = rs["subjectAreaLabel"]["value"] if "subjectAreaLabel" in rs else None
        most_specific_type = rs["mostSpecificType"]["value"] if "mostSpecificType" in rs else None
        doi = rs["doi"]["value"] if "doi" in rs else None

        if "uri" not in info:
            info = {"uri": publication, "title": title, "dcoId": dco_id}

        if publication_year:
            info.update({"publicationYear": int(publication_year)})

        if doi:
            doi = doi.replace("doi:", "")
            info.update({"doi": doi})

        if most_specific_type:
            info.update({"mostSpecificType": most_specific_type})

        if community:
            info.update({"community": {"uri": community, "name": community_name}})

        if venue:
            info.update({"presentedAt": {"uri": venue, "name": venue_name}})

        if abstract:
            info.update({"abstract": abstract})

        if subject_area:
            if "subjectArea" not in info:
                info["subjectArea"] = [{"uri": subject_area, "name": subject_area_name}]
            else:
                if not any(subject_area in sa["uri"] for sa in info["subjectArea"]):
                    info["subjectArea"].append({"uri": subject_area, "name": subject_area_name})
    return info


def get_publication_authors(publication):
    _query = get_publication_authors_query.replace("{publication}", "<" + publication + ">")
    r = query(_query)

    authors = {}

    for rs in r:
        name = rs['name']['value'].strip()
        rank = rs['rank']['value'] if 'rank' in rs else None
        uri = rs['uri']['value'].strip()
        researchArea = rs['researchArea']['value'].strip() if 'researchArea' in rs else None

        if name not in authors:
            authors.update({name: {"name": name, "uri": uri, "rank": rank}})

        if researchArea:
            if "researchArea" not in authors[name]:
                authors[name]["researchArea"] = [researchArea]
            else:
                authors[name]["researchArea"].append(researchArea)

    author_list = [author for name, author in authors.items()]

    try:
        ranked_author_list = sorted(author_list, key=lambda a: a["rank"])
        for a in ranked_author_list:
            a.pop("rank")
        return ranked_author_list
    except TypeError:
        return author_list


### Main ###

records = []

for publication in get_publications():
    pub = get_publication_info(publication)
    authors = get_publication_authors(publication)

    if authors:
        pub.update({"authors": authors})

    if "dcoId" in pub and pub["dcoId"] is not None:
        records.append(json.dumps(get_metadata(get_id(pub["dcoId"]))))
        records.append(json.dumps(pub))
    else:
        print(publication)

with open("publications.bulk", "w") as bulk_file:
    bulk_file.write('\n'.join(records))

