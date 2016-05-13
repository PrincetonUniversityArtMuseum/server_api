#loads the contents of the lib folder to elastic search
from elasticsearch import Elasticsearch
es = Elasticsearch()

import json
import requests
import urllib

import pprint
pp = pprint.PrettyPrinter(indent=4)
#global defs
port = 'http://localhost:9200'
database = 'puamapi'
wrapperbase = 'puamapi_wrapper'

#maps each table name to its associated id
api_tables = [
    "apiobjects_american", 
    "apiconstituents_american"
    #"apibibliography_american", 
    #"apiexhibitions_american"

    #"apiobjgeograph_american"
]

api_xrefs = [
    "apiobjconxrefs_american", 
    "apiobjbibxrefs_american", 
    "apiobjexhxrefs_american",

    "apiobjdimxrefs_american",
    "apiobjtermsxrefs_american",
    "apiobjgeograph_american",
    "apiobjmediaxrefs_american",
    "apiobjtitlexrefs_american"

]

table_xref_map = {
    "apiobjects_american": [
        "apiconstituents_american", 
        "apibibliography_american", 
        "apiexhibitions_american",
        
        "apiobjdimxrefs_american",
        "apiobjgeograph_american",
        "apiobjtermsxrefs_american",
        "apiobjtitlexrefs_american",
        "apiobjmediaxrefs_american"
    ], 
    
    "apiconstituents_american": [
        "apiobjects_american"
    ]
}

xref_map = {
    "apiobjconxrefs_american":[
        "apiobjects_american", 
        "apiconstituents_american"
    ], 

    "apiobjbibxrefs_american":[
        "apiobjects_american", 
        "apibibliography_american"
    ],

    "apiobjdimxrefs_american":[
        "apiobjects_american",
        "apiobjdimxrefs_american"
    ],

    "apiobjexhxrefs_american":[
        "apiobjects_american", 
        "apiexhibitions_american"
    ],

    "apiobjtermsxrefs_american":[
        "apiobjects_american",
        "apiobjtermsxrefs_american"
    ],

    "apiobjgeograph_american":[
        "apiobjects_american",
        "apiobjgeograph_american"
    ],


    "apiobjtitlexrefs_american":[
        "apiobjects_american",
        "apiobjtitlexrefs_american"
    ],

    "apiobjmediaxrefs_american":[
        "apiobjects_american",
        "apiobjmediaxrefs_american"
    ]
}

name_map = {
    "apiobjects_american":"Objects", 
    "apiconstituents_american":"Constituents", 
    "apibibliography_american":"Bibliography", 
    "apiexhibitions_american":"Exhibitions",
    #DimItemElemXrefID
    "apiobjdimxrefs_american": "Dimensions",
    #MediaxrefID
    "apiobjmediaxrefs_american": "Media",
    #ThesXrefID
    "apiobjtermsxrefs_american":"Terms",
    "apiobjtitlexrefs_american":"Titles",
    "apiobjgeograph_american":"Geography"
}

id_map = {
    "apiobjects_american":"ObjectID", 
    "apiconstituents_american": "ConstituentID", 
    "apiobjconxrefs_american": "ConXrefID", 
    "apibibliography_american":"ReferenceID", 
    "apiobjbibxrefs_american":"RefXRefID", 
    "apiexhibitions_american":"ExhibitionID", 
    "apiobjexhxrefs_american":"ExhObjXrefID",

    "apiobjdimxrefs_american":"DimensionID",
    "apiobjmediaxrefs_american":"MediaMasterID",
    "apiobjtermsxrefs_american":"TermID",
    "apiobjtitlexrefs_american":"TitleID",
    "apiobjgeograph_american":"ObjGeographyID"
}

#returns an array of json objects for each item in the array
#ex: get_xref_data("apiobjconxrefs_american", "apiobjects", "2497")
#def get_xref_data(xref, table, id):
    #if id == None:
        #return []

    #id_name = id_map[table]
    #query_path = port + "/" + database + "/" + xref + "/" + "_search?q=" + id_name + ":" + str(id)

    #request = requests.get(query_path).json()
    #request = json.load(urllib.urlopen(query_path))["hits"]["hits"]
    #data = requests.get(query_path).json()["hits"]["hits"]["_source"]

    #result = []
    #for item in request:
        #cur_item = {}
        #cur_item["ConstituentID"] = item["_source"]["ConstituentID"]
        #cur_item["DisplayName"] = item["_source"]["DisplayName"]

        #result.append(cur_item)
    #return result

#takes in an array of objects and loads them
def load_table(table, records):
    id_name = id_map[table]
    #returns path of format
    #http://localhost:9200/puamapi/apiobjects_american/
    port_path = ''.join([port, "/", database, "/", table, "/"])

    for record in records:    
        id = record[id_name]
        if id is not None: 
        #query each xref
        #for i in range(0, len(api_obj_xrefs)):
            #name = api_obj_xrefs_names[i]
            #record[name] = get_xref_data(api_obj_xrefs[i], table, id)

        #put the record in its file name 
            if table in table_xref_map:
                for xref in table_xref_map[table]:
                    record[name_map[xref]] = []

            es.index(index=database, doc_type=table, id=id, body=record)

    print("DONE")

#for each table in tables, reads the file and down
def load_tables(tables):
    for table in tables:
        print("loading table " + table)
        file_name = ''.join(["lib/", table, ".json"])
        with open(file_name) as file:
            load_table(table, json.load(file)["RECORDS"])

def load_wrapper(table, records):
    idname = id_map[table]

    for record in records:
        record_id = record[idname]
        print(record_id)
        #exists in puamapi
        if record_id is not None and es.exists(index=database, doc_type=table, id=record_id):
            #we get the record
            wrapper = es.get(index=database, doc_type=table, id=record_id)["_source"]
            errors = {}

            #for each xref
            for xref in table_xref_map[table]:
                xref_name = name_map[xref]
                items = wrapper[xref_name]
                for i in xrange(len(items)):
                    xref_id = items[i]
                    if es.exists(index=database, doc_type=xref, id=xref_id):
                        xref_item = es.get(index=database, doc_type=xref, id=xref_id)["_source"]
                        items[i] = xref_item
                    #uh oh, reference not detected in database
                    else:
                        #remove the reference from the array
                        if xref_name not in errors:
                            errors[xref_name] = [items[i]]
                        else:
                            errors[xref_name].append(items[i])

                #we remove the errors from the array
                if xref_name in errors:
                    for error in errors[xref_name]:
                        items.remove(error)

            wrapper["Errors"] = errors

            #then we put in the wrapper database
            es.index(index=wrapperbase, doc_type=table, id=record_id, body=wrapper)

    print("DONE")
#takes in an array of objects and loads them
def load_xref(xref, table1, table2, records):
    #http://localhost:9200/puamapi/apiobjects_american/
    idname1 = id_map[table1]
    idname2 = id_map[table2]
    
    name1 = name_map[table1]
    name2 = name_map[table2]
  

    script1 = "ctx._source." + name1 + " += item"
    script2 = "ctx._source." + name2 + " += item"

    update1 = (table1 in table_xref_map and table2 in table_xref_map[table1])
    update2 = (table2 in table_xref_map and table1 in table_xref_map[table2])

    #script = "if (ctx._source[" + name2 + "] == null) {ctx._source." + name2 +  " = item } else { ctx._source." + name2 + "+= item }"

    for record in records:    
        id1 = record[idname1]
        id2 = record[idname2]
        
        if update1 and  es.exists(index=database, doc_type=table1, id=id1):
            print(es.get(index=database, doc_type=table1, id=id1))
            #put the record in its file name 
            data2 = {}
            data2["item"] = [id2]
            record1 = {}
            record1["script"] = script2
            record1["params"] = data2


            es.update(index=database, doc_type=table1, id=id1, body=record1)

        if update2 and es.exists(index=database, doc_type=table2, id=id2):
            data1 = {}
            data1["item"] = [id1]
            record2 = {}
            record2["script"] = script1
            record2["params"] = data1

            es.update(index=database, doc_type=table2, id=id2, body=record2)

#makes a "complete" structure in puamapi_wrapper
def load_wrappers(tables):
    for table in tables:
        print("loading wrapper " + table)
        file_name = ''.join(["lib/", table, ".json"])
        with open(file_name) as file:
            json_file = json.load(file)
            load_wrapper(table, json_file["RECORDS"])

#for each table in tables, reads the file and down
def load_xrefs(tables):
    for table in tables:
        print("loading xref " + table)
        file_name = ''.join(["lib/", table, ".json"])
        with open(file_name) as file:
            json_file = json.load(file)
            print(xref_map[table])
            load_xref(table, xref_map[table][0], xref_map[table][1], json_file["RECORDS"])

#for each table, gets the file from github and writes it
def pull_tables(tables):
    base = "https://raw.githubusercontent.com/PrincetonUniversityArtMuseum/json_database/master"

    for table in tables:
        file_name = ''.join([table, ".json"]) #json filetype

        #https://raw.githubusercontent.com/american-art/PUAM/master/apiobjects_american/apiobjects_american.json
        url_path = ''.join([base, "/",  table, "/", file_name])
        file_path = ''.join(["lib/", file_name])

        print("retrieving " + url_path + " to " + file_path)
        urllib.urlretrieve(url_path, file_path)
        print("successful")

pull_tables(api_tables)
pull_tables(api_xrefs)
load_tables(api_xrefs)
#load_tables(["apibibliography_american", "apiexhibitions_american"])
load_tables(api_tables)
#load_xrefs(["apiobjtermsxrefs_american", "apiobjgeograph_american", "apiobjmediaxrefs_american", "apiobjtitlexrefs_american"])
load_wrappers(api_tables)

#load_xrefs(api_xrefs)

