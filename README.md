# MySQL binlog cat tool

## Introduction

binlogcat is a tool to show row events (insert/update/delete) 
in MySQL binlog file. The log would be shown to stdout in 
[maxwell](http://maxwells-daemon.io/dataformat/) format:

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

To install it in golang 1.10 version, run:

```
git pull github.com/kevinclcn/binlogcat

cd binlogcat

vgo build


```

## Usage

1. Add configuration below to a configuration file (such as config.yml) and change the fields on your need.

```yaml
host: 127.0.0.1
port: 3306
user: root
password: root
binlog: ~/Downloads/mysql-bin.005883
schema: xiaoshu
tables:
  - customer
```

2. run below commands


```
./binlogcat -c ./config.yml | jq .

```



