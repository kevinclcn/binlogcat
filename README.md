# MySQL binlog parser

go version > 1.11

## Introduction

parsebinlog is used to parse mysql insert/update/delete row events in MySQL binlog file. 

The parsing result would be written to stdout, the parsing error/info would be written to stderr.

The parsing result is formatted after [maxwell](http://maxwells-daemon.io/dataformat/)'s format:

- INSERT

```json
{
   "database":"test",
   "table":"e",
   "type":"insert",
   "ts":1477053217,
   "server_id":23042,
   "data":{
      "id":1,
      "m":4.2341,
      "c":"2016-10-21 05:33:37.523000",
      "comment":"I am a creature of light."
   }
}

```

- UPDATE

```json
{
   "database":"test",
   "table":"e",
   "type":"update",
   "ts":1477053234,
   "data":{
      "id":1,
      "m":5.444,
      "c":"2016-10-21 05:33:54.631000",
      "comment":"I am a creature of light."
   },
   "old":{
      "m":4.2341,
      "c":"2016-10-21 05:33:37.523000"
   }
}

```

- DELETE

```json
{
   "database":"test",
   "table":"e",
   "type":"delete",
   "data":{
      "id":1,
      "m":5.444,
      "c":"2016-10-21 05:33:54.631000",
      "comment":"I am a creature of light."
   }
}
```

## Installation

```
go get github.com/kevinclcn/parsebinlog

```

## Usage

### parse binlog files
```
./parsebinlog -user root -password root -db xiaoshu -tables customer -files {binlog file or directory}

```

### parse binlog http urls

```
echo "http://binlog.download.url" | ./parsebinlog -user root -password root -db xiaoshu -tables customer

```




