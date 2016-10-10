/*
 * Worker thread to save records to a DynamoDB table
 */

var AWS = require('aws-sdk');
 
AWS.config.update({
    region: "us-east-1",
    endpoint: "http://localhost:8000"
});

var docClient = new AWS.DynamoDB.DocumentClient();

var debug = false;

process.on('message', function(params) {
  if (debug) console.log('[worker] received message from server:', process.pid);
  //  console.log(JSON.stringify(params));
  if (params) {
     docClient.put(params, function(err, data) {
        if (err) {
          console.log("[worker]",process.pid, "Error saving item:", process.pid, err);
          console.log(params);
        } else {
          if (debug) console.log("[worker]", process.pid, "Saved item", params["Item"]);
        }
        process.send({
          worker  : process.pid,
          data    : (err ? err : data),
          success : (err === null)
        });
     });
  } else {
    console.log("[worker] ", process.pid, " done"); 
    process.send({
       worker  : process.pid,
       data    : null,
       success : false 
    });
  }
});
