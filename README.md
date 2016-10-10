#load-dyno-table
Load an AWS DynamoDB table from a JSON data file using multiple threads.

This allows you to load a table without using AWS pipelining.

##Synopsis
`$ load-dyno-table -t TableName -d DataFile.json <options>`

##Options
| Alias | Option | Default | Description |
| :---- | :----- | :------ | :---------- |
| -t | --tableName *name* | | Name of DynamoDB table to load |
| -d | --dataFile *file* | | JSON data file containing an array of records to load |
| -c | --createTable *file* | | Create a new table using definition in Node module |
| -r | --removeTable | | Delete existing table (requires -c) |
| -g | --region *region* | us-east-1 | AWS Region |
| -u | --endpoint *url* | http://localhost:8000 | DynamoDB instance URL |
| -v | --debug | false | Show verbose debugging information |
| -h | --help | | Show program usage |
