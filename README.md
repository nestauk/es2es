# es2es [![Build Status](https://travis-ci.com/nestauk/es2es.svg?branch=master)](https://travis-ci.com/nestauk/es2es)

Transfers data between Elasticsearch servers

## Installation

```python

pip install es2es
```

## Usage

```python

from es2es import es2es

es2es(origin_endpoint='https://PATH-TO-ENDPOINT',
      origin_index='ORIGIN_INDEX',
      dest_endpoint='https://PATH-TO-ANOTHER-ENDPOINT',
      dest_index='DESTINATION_INDEX')
```

## Advanced usage

```python

origin_endpoint (str): "Path to the endpoint to transfer data from."
origin_index (str): "Index at the endpoint to transfer data from."
dest_endpoint (str): "Path to the endpoint to transfer data to."
dest_index (str): "Index at the endpoint to transfer data to."
origin_method (str, default='GET'): "Method for retrieving data from origin endpoint (e.g. 'GET' or 'POST')."
chunksize (int, default=100): "Number of records to send to the new server in bulk."
scroll (str, default='1m'): "Elasticsearch scroll window."
origin_kwargs (dict, default={}): "Any additional kwargs to add to the http(s) request to the origin endpoint."
dest_kwargs (dict, default={}): "Any additional kwargs to add to the http(s) request to the origin endpoint."
do_transfer_index (bool, default=True): "Flag regarding whether to transfer the index, settings and mapping as well. Set this to False if the index already exists, or you're restarting a transfer."
limit (int, default=None): "Maximum number of 'rows' of data to transfer."
```
