import urllib.parse
import requests
import json


class ElasticsearchError(Exception):
    pass


def unpack_if_safe(r):
    """
    Check status and ES response for bad behaviour, and
    return json if not problems detected.
    """
    if r.status_code not in (200, 400):
        r.raise_for_status()
    data = json.loads(r.text)
    if 'error' in data:
        root_cause = data['error']['root_cause'][0]
        reason = root_cause['reason']
        error_type = root_cause['type']
        index = root_cause['index']
        status = data['status']
        raise ElasticsearchError(f"{status} response "
                                 "from ES with error of type "
                                 f"'{error_type}' after query "
                                 f"on index '{index}' with "
                                 "request body\n\n"
                                 f"{r.request.body}\n\n"
                                 f"with reason\n\n'{reason}'.")
    return data


def make_url(endpoint, index, api):
    """
    Join the endpoint, index and API method together.
    """
    url = '/'.join((endpoint, index, api))
    while '//' in url:
        url = url.replace('//', '/')
    if url.endswith('/'):
        url = url[:-1]
    return url.replace(':/', '://')


def request(endpoint, index, method, api='',
            data={}, **kwargs):
    """
    Make a request to ES via a specified method and API.
    """
    # Don't accept DELETE, that's mental
    if method == 'DELETE':
        raise ValueError('es2es does not support DELETE. '
                         'Do that on your own watch.')
    # Set basic headers if not otherwise specified
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    if 'Content-Type' not in kwargs['headers']:
        kwargs['headers']['Content-Type'] = 'application/json'
    # Formulate and make the request
    url = make_url(endpoint, index, api)
    _method = getattr(requests, method.lower())
    if type(data) is dict:
        data = json.dumps(data)
    r = _method(url, data=data, **kwargs)
    return unpack_if_safe(r)


def transfer_index(origin_endpoint, origin_index,
                   dest_endpoint, dest_index,
                   origin_method, origin_kwargs,
                   dest_kwargs):
    """
    Transfer the index and mapping of one ES index/endpoint
    to another
    """
    if ((origin_endpoint == dest_endpoint) and (origin_index == dest_index)):
        raise ValueError("es2es won't attempt to reindex the"
                         "index {origin_method} to itself")

    # Get the settings from the origin index
    settings = request(endpoint=origin_endpoint,
                       index=origin_index,
                       method=origin_method,
                       **origin_kwargs)
    idx = settings[origin_index]['settings']['index']
    for k in ('creation_date', 'uuid', 'version',
              'provided_name'):
        idx.pop(k)
    # PUT the settings into the new index
    request(endpoint=dest_endpoint,
            index=dest_index,
            method='PUT',
            data=settings[origin_index],
            **dest_kwargs)


def extract_data(endpoint, index, method,
                 chunksize, scroll, **kwargs):
    """
    Get all data from a given endpoint/index, in chunks.
    """
    # Make initial empty query
    data = request(endpoint=endpoint, index=index,
                   params={'scroll': scroll},
                   data={'size': chunksize},
                   method=method, api='_search',
                   **kwargs)
    docs = data['hits']['hits']
    yield docs

    scroll_data = json.dumps({'scroll': scroll,
                              'scroll_id': data['_scroll_id']})
    while len(docs) == chunksize:
        data = request(endpoint=endpoint, index='_search',
                       api='scroll', data=scroll_data,
                       method=method, **kwargs)
        docs = data['hits']['hits']
        yield docs


def format_bulk_docs(docs):
    """
    Format all rows in chunk, ready for bulk upload.
    """
    data = []
    for doc in docs:
        doc.pop('_score')
        doc.pop('_index')
        source = doc.pop('_source')
        data += [json.dumps({'index': doc}), json.dumps(source)]
    return '\n'.join(data) + '\n'


def transfer_data(origin_endpoint, origin_index,
                  dest_endpoint, dest_index,
                  origin_method, origin_kwargs,
                  dest_kwargs, chunksize, scroll,
                  limit=None):
    """
    Transfer data from one endpoint to another.
    """
    n_found = 0
    for chunk in extract_data(endpoint=origin_endpoint,
                              index=origin_index,
                              method=origin_method,
                              chunksize=chunksize,
                              scroll=scroll, **origin_kwargs):
        do_break = False
        if limit is not None:
            if n_found + len(chunk) >= limit:
                do_break = True
                chunk = chunk[:limit - n_found]
        n_found += len(chunk)

        data = format_bulk_docs(chunk)
        request(endpoint=dest_endpoint,
                index=dest_index, api='_bulk',
                data=data, method='POST', **dest_kwargs)

        if do_break:
            break


def es2es(origin_endpoint, origin_index, dest_endpoint,
          dest_index=None, origin_method='GET', chunksize=100,
          scroll='1m', origin_kwargs={}, dest_kwargs={},
          do_transfer_index=True, limit=None):
    """
    Transfer settings, mapping and data from
    one endpoint to another.
    """
    dest_index = (origin_index if dest_index is None
                  else dest_index)
    kwargs = dict(origin_endpoint=origin_endpoint,
                  origin_index=origin_index,
                  dest_endpoint=dest_endpoint,
                  dest_index=dest_index,
                  origin_method=origin_method,
                  origin_kwargs=origin_kwargs,
                  dest_kwargs=dest_kwargs)
    if do_transfer_index:
        transfer_index(**kwargs)
    transfer_data(chunksize=chunksize, scroll=scroll,
                  limit=limit, **kwargs)
