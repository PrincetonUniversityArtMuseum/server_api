# Princeton Art Museum API


This documentation summarizes the API built for the Princeton University Art Museum in 2015-2016.


## Introduction
The art museum has tons of data on objects, constituents, etc. in SQL databases. We wanted to create a web service that would return results from queries to tables of this database in JSON form quickly. 

## Components
We approached the problem in two ways.
* First, a Golang-based API that communicates directly with the SQL instance (located [here](https://github.com/cjqian/princeton_museum_api)). This has the advantage of communicating direcly with the SQL server without need for cached files, but ultimately was too slow to be an efficient service with our large amount of data. Documentation on this is a work in progress.

* Currently, we're using a Python-based [API](https://github.com/PrincetonUniversityArtMuseum/server_api) that works with a cached, single-level JSON library. This library is pulled from [here](https://github.com/PrincetonUniversityArtMuseum/json_database), which is manually pulled from the SQL instance. We hope to automate this in the future. 

## Contact
Cathryn Goodwin (cathryng@princeton.edu) is in charge of Collections, and Crystal Qian (cqian@princeton.edu) wrote the source code for these APIs. Feel free to e-mail with any questions. 
