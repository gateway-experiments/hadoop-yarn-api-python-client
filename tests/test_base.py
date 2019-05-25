# -*- coding: utf-8 -*-
try:
    from httplib import OK # NOQA
except ImportError:
    from http.client import OK # NOQA

import json
import requests_mock

from tests import TestCase
from yarn_api_client import base
from yarn_api_client.errors import APIError, ConfigurationError


class BaseYarnAPITestCase(TestCase):
    @staticmethod
    def success_response():
        return {
            'status': 'success'
        }

    def test_valid_request(self):
        with requests_mock.mock() as requests_get_mock:
            requests_get_mock.get('/ololo', text=json.dumps(BaseYarnAPITestCase.success_response()))

            client = self.get_client()
            response = client.request('/ololo', params={"foo": 'bar'})

            assert requests_get_mock.called
            self.assertIn(response.data['status'], 'success')

    def test_valid_request_with_parameters(self):
        with requests_mock.mock() as requests_get_mock:
            requests_get_mock.get('/ololo?foo=bar', text=json.dumps(BaseYarnAPITestCase.success_response()))

            client = self.get_client()
            response = client.request('/ololo', params={"foo": 'bar'})

            assert requests_get_mock.called
            self.assertIn(response.data['status'], 'success')

    def test_bad_request(self):
        with requests_mock.mock() as requests_get_mock:
            requests_get_mock.get('/ololo', status_code=404)

            client = self.get_client()
            with self.assertRaises(APIError):
                client.request('/ololo')

    def test_http_configuration(self):
        with requests_mock.mock() as requests_get_mock:
            requests_get_mock.get('/ololo', text=json.dumps(BaseYarnAPITestCase.success_response()))

            client = self.get_client()
            client.address = None
            client.port = 80

            with self.assertRaises(ConfigurationError):
                client.request('/ololo')

    def get_client(self):
        client = base.BaseYarnAPI()
        client.address = 'example.com'
        client.port = 80
        client.timeout = 0
        client.kerberos_enabled = False
        client.https = False
        client.https_verify = True
        return client
