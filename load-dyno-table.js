var AWS = require('aws-sdk');
var ProgressBar = require('progress');
var child_process = require('child_process');
var cpuCount  = require('os').cpus().length; 
var fs = require('fs');
var jsonfile = require('jsonfile');

var bar = null;
var debug = false;

var table = processCommandLine();

AWS.config.update({
    region: table.region,
    endpoint: table.endpoint
});

var dynamodb = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();

console.log("Loading table", table.tableName, "from", table.dataFile);
//var data = jsonfile.readFileSync(table.dataFile);
jsonfile.readFile(table.dataFile, function(err, fileData) {
   if (err) {
      errorHandler(err);
   } else {
      data = fileData;
      console.log("Read", data.length, "records from", table.dataFile);
      if (table.removeTable) 
         deleteTable().then(createTableHandler, errorHandler);
      else if (table.createTable) 
         createTableHandler();
      else 
         startWorkers();
   }
});


////////////////////////
   
function processCommandLine() {
  var options = [
    { name: 'tableName'   , alias: 't', type: String , typeLabel: '[underline]{name}'  , description: 'Name of DynamoDB table to load.'},
    { name: 'dataFile'    , alias: 'd', type: String , typeLabel: '[underline]{file}'  , description: 'JSON data file containing an array of records to load.'},
    { name: 'createTable' , alias: 'c', type: String , typeLabel: '[underline]{file}'  , description: 'Create a new table using definition in Node module.'},
    { name: 'removeTable' , alias: 'r', type: Boolean, typeLabel: ''                   , description: 'Delete existing table (requires -c).', defaultValue: false},
    { name: 'region'      , alias: 'g', type: String , typeLabel: '[underline]{region}', description: 'AWS Region (us-east-1).', defaultValue: 'us-east-1' },
    { name: 'endpoint'    , alias: 'u', type: String , typeLabel: '[underline]{url}'   , description: 'DynamoDB instance URL (local).', defaultValue: 'http://localhost:8000' },
    { name: 'debug'       , alias: 'v', type: Boolean, typeLabel: ''                   , description: 'Show verbose debugging information.' },
    { name: 'help'        , alias: 'h', type: Boolean, typeLabel: ''                   , description: 'Show program usage.' }

  ];
  var commandLineArgs = require('command-line-args');
  var table = commandLineArgs(options);
  if (table.help || !table.tableName || !table.dataFile) displayUsage(options);
  if (table.removeTable && !table.createTable) displayUsage(options);
  if (table.debug) debug = true;
  if (debug) console.log(table);
  return table;
}

function showBar() {
  if (!debug) {
    bar = new ProgressBar('Loading: [:bar] :percent :etas', { total: data.length});
  }
}

function getNextRecord() {
  if (!debug && bar) bar.tick();
  index += 1;
  if (index < data.length) {
     return {
         TableName: table.tableName,
         Item: data[index]
     };
  }
  return null;
}
 
function createTable() {
      return new Promise(function(resolve, reject) {
         if (debug) console.log("Creating table", table.tableName, "from", table.createTable);
         var tableDef = require(table.createTable)();
         if (debug) console.dir(tableDef);
         dynamodb.createTable(tableDef, function(err, data) {
           if (err)
             reject(err);
           else 
             resolve(data);
         });
   });
}
 
function deleteTable() {
   return new Promise(function(resolve, reject) {
      if (debug) console.log("Removing table ", table.tableName);
      var params = {
          TableName: table.tableName
      };
      dynamodb.deleteTable(params, function(err, data) {
        if (err)
          reject(err);
        else
          resolve(data);
      });
   });
}

function createWorker() {
  var child = child_process.fork('./load-dyno-worker');
  console.log("[parent] Creating worker ", child.pid);
  child.on('message', function(message) {
    if (debug) console.log(message);
    if (message.success) { 
      child.send(getNextRecord());
    } else {
      child.disconnect();
    }
  });
  return child;
}

function startWorkers() {
   index = -1;
   workerCnt = (cpuCount < 2 ? 1 : cpuCount - 1);
   for (i = 1; i <= workerCnt; i++) {
     var child = createWorker();
     child.send(getNextRecord());
   }
   showBar();
}

function errorHandler(error) {
   console.log("Error", error);
   process.exit(2);
}

function createTableHandler() {
  createTable().then(startWorkers, errorHandler);
}

function displayUsage(options) {
  var getUsage = require('command-line-usage');
  var sections = [
    {
      header: 'load-dyno-table',
      content: 'Load an AWS DynamoDB table from a JSON data file.'
    },
    {
      header: 'Synopsis',
      content: '$ load-dyno-table <options> [bold]{-t} [underline]{TableName} [bold]{-d} [underline]{DataFile.json}'
    },     
    {
      header: 'Options',
      optionList: options 
    }
  ];
  var usage = getUsage(sections);
  console.log(usage);
  process.exit(1);
}

