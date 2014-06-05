# -*- coding: utf-8 -*-
from httplib import OK

from mock import patch
from unittest import TestCase

from yarn_api_client import base


class BaseYarnAPITestCase(TestCase):
    def test_http_property_cache(self):
        client = self.get_client()
        http_conn1 = client.http_conn
        http_conn2 = client.http_conn

        self.assertIs(http_conn1, http_conn2)

    def test_request(self):
        client = self.get_client()
        with patch('yarn_api_client.base.HTTPConnection') as http_conn_mock:
            with patch('yarn_api_client.base.json'):
                http_conn_mock().getresponse().status = OK

                client.request('/ololo', foo='bar')

                http_conn_mock().request.assert_called_with('GET', '/ololo?foo=bar')

    def get_client(self):
        client = base.BaseYarnAPI()
        client.address = 'example.com'
        client.port = 80
        client.timeout = 0
        return client
