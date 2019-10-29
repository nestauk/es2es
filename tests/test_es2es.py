from requests.exceptions import HTTPError

import mock
import pytest

from _es2es import unpack_if_safe
from _es2es import ElasticsearchError
from _es2es import request
from _es2es import transfer_index
from _es2es import extract_data
from _es2es import format_bulk_docs
from _es2es import make_url


def test_make_url():
    assert make_url('http://a', 'b', 'c') == 'http://a/b/c'
    assert make_url('https://a', 'b', 'c') == 'https://a/b/c'
    assert make_url('a', 'b', 'c') == 'a/b/c'
    assert make_url('a', '', 'c') == 'a/c'
    assert make_url('a', '', '') == 'a'
    assert make_url('a', 'b', '') == 'a/b'


def test_unpack_if_safe_bad_response():
    r = mock.MagicMock()
    r.raise_for_status.side_effect = lambda x: HTTPError()
    with pytest.raises(Exception):
        unpack_if_safe(r)


def test_unpack_if_safe_es_error_and_no_es_error():
    r = mock.MagicMock()
    r.text = '{"error":{"root_cause":[{"reason":0, "type":0, "index":0}]}, "status":400}'
    r.request.body = 'blah blah'
    with pytest.raises(ElasticsearchError):
        unpack_if_safe(r)
    r.text = '{"data":"something"}'
    assert unpack_if_safe(r) == {"data": "something"}


@mock.patch('_es2es.unpack_if_safe', side_effect=lambda x: x)
@mock.patch('_es2es.requests')
def test_request(mocked_requests, mocked_unpack):
    kwargs = dict(another_other_kwarg=None,
                  another_final_kwarg=23)
    request('the_endpoint', 'the_index', 'the_method',
            api='the_api', data={'the_data': 'the_value'},
            **kwargs)
    request('another_endpoint', 'another_index',
            'the_method', api='another_api',
            data='another_data', **kwargs)
    kwargs['headers'] = {'Content-Type': 'application/json'}
    expected_calls = [mock.call('the_endpoint/the_index/'
                                'the_api',
                                data=('{"the_data": '
                                      '"the_value"}'),
                                **kwargs),
                      mock.call('another_endpoint/'
                                'another_index/'
                                'another_api',
                                data="another_data",
                                **kwargs)]
    f = mocked_requests.the_method
    assert f.call_count == len(expected_calls)
    f.assert_has_calls(expected_calls)


@mock.patch('_es2es.request')
def test_transfer_index(mocked_request):
    transfer_index('origin_endpoint', 'origin_index',
                   'dest_endpoint', 'dest_index',
                   'origin_method', origin_kwargs={},
                   dest_kwargs={})
    mocked_request.call_count == 2


def test_transfer_index_bad_index():
    with pytest.raises(ValueError):
        transfer_index('origin_endpoint', 'origin_index',
                       'origin_endpoint', 'origin_index',
                       'origin_method', origin_kwargs={},
                       dest_kwargs={})


@mock.patch('_es2es.request')
def test_extract_data(mocked_request):
    first_doc = {'_scroll_id': 100, 'hits': {'hits': [1]*23}}
    last_doc = {'_scroll_id': 100, 'hits': {'hits': [1]*12}}
    returns = [first_doc]*123 + [last_doc]
    mocked_request.side_effect = returns
    for i, docs in enumerate(extract_data('an_endpoint',
                                          'an_index',
                                          'a_method', chunksize=23, scroll='1m')):
        if i == len(returns) - 1:
            assert len(docs) == 12
        else:
            assert len(docs) == 23
    assert mocked_request.call_count == len(returns)


def test_format_bulk_docs():
    docs = [{'_score': 23, '_index': None, 'id': 322,
             '_source': {'a_field': 43,
                         'another_field': 54}},
            {'_score': 'blah', '_index': 34, 'id': 'joel',
             '_source': {'a_field': 'klinger',
                         'another_field': 21}}]
    assert format_bulk_docs(docs) == (
        '{"index": {"id": 322}}\n'
        '{"a_field": 43, "another_field": 54}\n'
        '{"index": {"id": "joel"}}\n'
        '{"a_field": "klinger", "another_field": 21}\n')
    
