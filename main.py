#/loads the contents of the lib folder to elastic search
from elasticsearch import Elasticsearch
es = Elasticsearch()

import ujson
import requests
import urllib

#global defs
port = 'http://localhost:9200'
database = 'puamapi'
wrapperbase = 'puamapi_wrapper'

#maps each table name to its associated id
api_tables = [
    "apiobjects", 
    "apiconstituents",
    "apibibliography" 
    #"apiexhibitions"

    #"apiobjgeography"
]

api_xrefs = [
    # for objects
    "apiobjconxrefs", 
    "apiobjbibxrefs", 
    "apiobjexhxrefs",

    "apiobjdimxrefs",
    #"apiobjtermsxrefs",
    #"apiobjgeography",
    "apiobjmediaxrefs",
    "apiobjtitlexrefs",

    # for constituents 
    "apiconaltnames",
    "apiconuris",

    #for bibliography
    "apibibaltnums"
]

table_xref_map = {
    "apiobjects": [
        "apiconstituents", 
        "apibibliography", 
        "apiexhibitions",
        
        "apiobjdimxrefs",
      #  "apiobjgeography",
       # "apiobjtermsxrefs",
        "apiobjtitlexrefs",
        "apiobjmediaxrefs"
    ], 
    
    "apiconstituents": [
        "apiobjects",
        "apiconaltnames",
        "apiconuris"
    ],

    "apibibliography":[
	"apibibaltnums"
    ]
}

xref_map = {
#objects
    "apiobjconxrefs":[
        "apiobjects", 
        "apiconstituents"
    ], 

    "apiobjbibxrefs":[
        "apiobjects", 
        "apibibliography"
    ],

    "apiobjdimxrefs":[
        "apiobjects",
        "apiobjdimxrefs"
    ],

    "apiobjexhxrefs":[
        "apiobjects", 
        "apiexhibitions"
    ],

   # "apiobjtermsxrefs":[
   #     "apiobjects",
   #     "apiobjtermsxrefs"
   # ],

   # "apiobjgeography":[
   #     "apiobjects",
   #     "apiobjgeography"
   # ],


    "apiobjtitlexrefs":[
        "apiobjects",
        "apiobjtitlexrefs"
    ],

    "apiobjmediaxrefs":[
        "apiobjects",
        "apiobjmediaxrefs"
    ],

#constituents
    "apiconaltnames":[
    	"apiconstituents", 
	"apiconaltnames"
    ],

    "apiconuris":[
        "apiconstituents",
	"apiconuris"
    ],

    "apibibaltnums":[
	"apibibliography",
	"apibibaltnums"
    ]
}

name_map = {
    "apiobjects":"Objects", 
    "apiconstituents":"Constituents", 
    "apibibliography":"Bibliography", 
    "apiexhibitions":"Exhibitions",
    #DimItemElemXrefID
    "apiobjdimxrefs": "Dimensions",
    #MediaxrefID
    "apiobjmediaxrefs": "Media",
    #ThesXrefID
    #"apiobjtermsxrefs":"Terms",
    "apiobjtitlexrefs":"Titles",
    #"apiobjgeography":"Geography",

     #for constituents
    "apiconaltnames":"AltNames",
    "apiconuris":"URIs",

    #for bibliography
    "apibibaltnums":"AltBibs"
}

id_map = {
    "apiobjects":"ObjectID", 
    "apiconstituents": "ConstituentID", 
    "apiobjconxrefs": "ConXrefID", 
    "apibibliography":"ReferenceID", 
    "apiobjbibxrefs":"RefXRefID", 
    "apiexhibitions":"ExhibitionID", 
    "apiobjexhxrefs":"ExhObjXrefID",

    "apiobjdimxrefs":"DimensionID",
    "apiobjmediaxrefs":"MediaMasterID",
    #"apiobjtermsxrefs":"TermID",
    "apiobjtitlexrefs":"TitleID",
    #"apiobjgeography":"ObjGeographyID",

    #constituents
    "apiconaltnames":"AltNameID",
    "apiconuris":"AltNumID",

    #bibliography
    "apibibaltnums":"AltNumID"
}

#returns an array of json objects for each item in the array
#ex: get_xref_data("apiobjconxrefs", "apiobjects", "2497")
#def get_xref_data(xref, table, id):
    #if id == None:
        #return []

    #id_name = id_map[table]
    #query_path = port + "/" + database + "/" + xref + "/" + "_search?q=" + id_name + ":" + str(id)

    #request = requests.get(query_path).json()
    #request = ujson.load(urllib.urlopen(query_path))["hits"]["hits"]
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
    print("Loading table: " + table)

    id_name = id_map[table]
    #returns path of format
    #http://localhost:9200/puamapi/apiobjects/
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

    print("Load table " + table + ": SUCCESSFUL")

#for each table in tables, reads the file and down
def load_tables(tables):
    for table in tables:
        file_name = ''.join(["lib/", table, ".json"])
        with open(file_name) as file:
	    print("loading table " + file_name)
	    json_data = ujson.load(file)
            records = json_data["RECORDS"]
            load_table(table,records)

def load_wrapper(table, records):
    print("Loading wrapper: " + table)
    idname = id_map[table]

    for record in records:
        record_id = record[idname]
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

    print("Loading wrapper "+ table + ": SUCCESSFUL")
#takes in an array of objects and loads them
def load_xref(xref, table1, table2, records):
    print("Loading xref: " + xref)

    #http://localhost:9200/puamapi/apiobjects/
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
	print(id1)
        if update1 and  es.exists(index=database, doc_type=table1, id=id1):
            #put the record in its file name 
            data2 = {}
	    #data2["table"] = name2
            data2["item"] = [id2]
            record1 = {}
            #record1["script_file"] = "xref_script"
	    record1["script"] = script2
            record1["params"] = data2

            es.update(index=database, doc_type=table1, id=id1, body=record1)
            #es.update(index=database, doc_type=table1, id=id1, body=doc1)

        if update2 and es.exists(index=database, doc_type=table2, id=id2):
            data1 = {}
            #data1["table"] = name1
            data1["item"] = [id1]
            record2 = {}
	    record2["script"] = script1
            #record2["script_file"] = "xref_script"
            record2["params"] = data1

	    #doc2 = {"doc": record2}
            es.update(index=database, doc_type=table2, id=id2, body=record2)
            #es.update(index=database, doc_type=table2, id=id2, body=doc2)

    print("Loading xref " + xref + ": SUCCESSFUL")

#makes a "complete" structure in puamapi_wrapper
def load_wrappers(tables):
    for table in tables:
        file_name = ''.join(["lib/", table, ".json"])
        with open(file_name) as file:
            json_file = ujson.load(file)
            load_wrapper(table, json_file["RECORDS"])

#for each table in tables, reads the file and down
def load_xrefs(tables):
    for table in tables:
        file_name = ''.join(["lib/", table, ".json"])
        with open(file_name) as file:
            json_file = ujson.load(file)
            load_xref(table, xref_map[table][0], xref_map[table][1], json_file["RECORDS"])

#for each table, gets the file from github and writes it
def pull_tables(tables):
    base = "https://raw.githubusercontent.com/PrincetonUniversityArtMuseum/json_database/master"

    for table in tables:
        file_name = ''.join([table, ".json"]) #json filetype

        #https://raw.githubusercontent.com/american-art/PUAM/master/apiobjects/apiobjects.json
        url_path = ''.join([base, "/",  file_name])
        file_path = ''.join(["lib/", file_name])

	print("Pulling table: " + url_path + " to " + file_path)
        urllib.urlretrieve(url_path, file_path)
        print("Pulling table: successful")

#download from github
#pull_tables(api_tables)
#pull_tables(api_xrefs)

#load them into our database
#load_tables(api_tables)
#load_tables(api_xrefs)
#load external references
remaining = [
#    "apiobjtermsxrefs",
#    "apiobjgeography",
#    "apiobjmediaxrefs",
#    "apiobjtitlexrefs",

    # for constituents 
#    "apiconaltnames",
#    "apiconuris",

    #for bibliography
#    "apibibaltnums"
    # for objects
#    "apiobjconxrefs", 
#    "apiobjbibxrefs", 
#    "apiobjexhxrefs",

#    "apiobjdimxrefs",
    #"apiobjtermsxrefs",
#    "apiobjgeography",
#    "apiobjmediaxrefs",
#    "apiobjtitlexrefs",


    "apiobjmediaxrefs",
    "apiobjtitlexrefs",

    # for constituents 
    "apiconaltnames",
    "apiconuris",

    #for bibliography
    "apibibaltnums"

]

#load_tables(remaining)
#load_tables(remaining)
load_xrefs(api_xrefs)
#load_xrefs(remaining)
#load the wrappers
load_wrappers(api_tables)


