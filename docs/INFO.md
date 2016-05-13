# Elasticsearch API

This documentation discusses in-detail the specifics of the Python API currently running on AWS.

## Specifics
We use the following tools:
* A **MySQL** instance converts tables to single-layered JSON.
* **Github** hosts the resultant [JSON](https://github.com/PrincetonUniversityArtMuseum/json_database) from these tables.
* Scripting is done in **Python 2.7**.
* The API runs on an **AWS EC2** instance.
* An **elasticserach 2.3.2** server handles JSON queries.
* **Kibana 4.1** provides visualizations and server statistics. 

## Process

**Section in progress**

## Usage Examples
###Elasticsearch
As mentioned before, there are two indices on our instance. `puamapi/` contains IDs to linked data, whereas `puamapi_wrapper` actually contains the linked data nested in the JSON output. 

#### Item Queries
```
52.38.52.6:9200/puamapi_wrapper/apiconstituents_american/4742
```
This will return a constituent object with ID 4742. Remember that only **constituents** and **objects** documents have items in the `puamapi_wrapper` index.

```
52.38.52.6:9200/puamapi/apiobjects_american/3293
```
This will return the object with ID 3293.

#### General Queries
You can also check other things, like the mappings (column to type) of each document. For example:

```
52.38.52.6:9200/puamapi/apiobjtermsxrefs/_mapping
```
This will return the types ("string," etc. for each column of the **apiobjtermsxrefs** table).

Or, you might want general information about the running index.
```
52.38.52.6:9200/puamapi/_stats
```

Further information on output, database structure and layout can be found [here](https://github.com/PrincetonUniversityArtMuseum/server_api/TABLES.md). 

### Kibana
 **Section in progress**
 
### Next steps
Notice that we have a **Node.js** Javascript file in this repository with an HTML default template and sample query. The next step is to build a simple interface for a user to send requests to the Elasticsearch instance without sending RESTful queries through the URL and also process the data in a more readable form.

## Troubleshooting/Maintenance
The following section assumes you have access to a "CDIdev.pem" private key and credentials for the TMSUser AWS account.

### AWS
Currently, we have one running EC2 [instance](52.38.52.6). To SSH into this Ubuntu instance from your terminal, 

```
ssh -i CDIdev.pem ubuntu@ec2-52-38-52-6.us-west-2.compute.amazonaws.com
```

You need to be in the same directory as the `CDIdev.pem` key for this to work.

Once in, you'll see all the documents/files for this API in the root folder.

#### Elasticsearch
You can make sure that the Elasticsearch instance is running by clicking [here](52.38.52.6:9200). If the instance is running, you will see JSON output. 

If the page fails to load, we need to restart the elastic search instance.

Navigate to the elastic search directory:
```
cd /usr/local/elasticsearch/elasticsearch2.3.2
```

From there, we need to start the instance.
```
./bin/elasticsearch
```

You should see initialization output on the console. When you see a notification that the status has turned from **red** to **yellow** and that indices have been recovered, you're good to go! The instance should be running now. 

Other commands that might be helpful:
* Shutting down the instance.
```
curl -XPOST 'http://localhost:9200/_shutdown'
```
* Nuking the instance. (That is, removing all data.) **This is most likely a bad decision. Proceed with EXTREME caution.**
```
curl -XDELETE 'http://localhost:9200/puamapi'
```
You will get an `acknowledged:true` notification after this query.

### Kibana
Not sure if Kibana is running? Click [here](http://52.38.52.6:5601/) to check.

If the page doesn't load, we need to restart the instance. First, navigate to the directory.
```
cd /opt/kibana/
```

Then, start the instance.
```
./bin/kibana
```

Theoretically, these instances should remain online after you log out of the AWS instance. If this doesn't work, though, consider running in the background or disowning the processes.

If all else fails, shoot me an e-mail!







`
