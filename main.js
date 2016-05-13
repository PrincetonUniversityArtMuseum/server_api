var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'trace'
});

/*
   client.ping({
// ping usually has a 3000ms timeout
requestTimeout: Infinity,

// undocumented params are appended to the query string
hello: "elasticsearch!"
}, function (error) {
if (error) {
console.trace('elasticsearch cluster is down!');
} else {
console.log('All is well');
}
});
*/

var search = function(term) {
    var query = {
      match: {
        _all: term
      }
    };

    client.search({
      index: 'puamapi_wrapper',
      type: 'apiobjects_american',
      body: {
        size: 5000,
        from: 0 * 10,
        query: query
      }
    }).then(function(result) {
      var ii = 0, hits_in, hits_out = [];

      hits_in = (result.hits || {}).hits || [];

      for(; ii < hits_in.length; ii++) {
        hits_out.push(hits_in[ii]._source);
        console.log(hits_in[ii]._source);
      }
    });


  };

search("silver", 0);
/*
client.search({
    index: 'puamapi_wrapper',
    type: 'apiobjects_american',
    body: {
        query: {
            match: {
                body: 'silver'
            }
        }
    }
    //classification: "Drawings"
}).then(function (resp) {
    var hits = resp.hits.hits;
}, function (err) {
    console.trace(err.message);
})a*/;
