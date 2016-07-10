# -*- coding: utf-8 -*-
try:
    from httplib import OK
except ImportError:
    from http.client import OK

from mock import patch
from tests import TestCase

from yarn_api_client import base
from yarn_api_client.errors import APIError, ConfigurationError


class BaseYarnAPITestCase(TestCase):
    def test_request(self):
        client = self.get_client()
        with patch('yarn_api_client.base.HTTPConnection') as http_conn_mock:
            with patch('yarn_api_client.base.json'):
                http_conn_mock().getresponse().status = OK

                client.request('/ololo', foo='bar')

                http_conn_mock().request.assert_called_with('GET', '/ololo?foo=bar')

                http_conn_mock.reset_mock()
                client.request('/ololo')

                http_conn_mock()

                http_conn_mock().request.assert_called_with('GET', '/ololo')

    def test_bad_request(self):
        client = self.get_client()
        with patch('yarn_api_client.base.HTTPConnection') as http_conn_mock:
            http_conn_mock().getresponse().status = 404

            with self.assertRaises(APIError):
                client.request('/ololo')
            
    def test_http_configuration(self):
        client = self.get_client()
        client.address = None
        client.port = 80

        with self.assertRaises(ConfigurationError):
            conn = client.http_conn

        client.address = 'localhost'
        client.port = None

        with self.assertRaises(ConfigurationError):
            conn = client.http_conn

    def get_client(self):
        client = base.BaseYarnAPI()
        client.address = 'example.com'
        client.port = 80
        client.timeout = 0
        return client
