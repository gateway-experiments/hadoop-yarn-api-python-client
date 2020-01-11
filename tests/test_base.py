# -*- coding: utf-8 -*-
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
            client.service_uri = None

            with self.assertRaises(ConfigurationError):
                client.request('/ololo')

    def test_uri_parsing(self):
        result_uri = base.Uri('localhost')
        self.assertEqual(result_uri.scheme, 'http')
        self.assertEqual(result_uri.hostname, 'localhost')
        self.assertEqual(result_uri.port, None)
        self.assertEqual(result_uri.is_https, False)

        result_uri = base.Uri('test-domain.com:1234')
        self.assertEqual(result_uri.scheme, 'http')
        self.assertEqual(result_uri.hostname, 'test-domain.com')
        self.assertEqual(result_uri.port, 1234)
        self.assertEqual(result_uri.is_https, False)

        result_uri = base.Uri('http://123.45.67.89:1234')
        self.assertEqual(result_uri.scheme, 'http')
        self.assertEqual(result_uri.hostname, '123.45.67.89')
        self.assertEqual(result_uri.port, 1234)
        self.assertEqual(result_uri.is_https, False)
        
        result_uri = base.Uri('https://test-domain.com:1234')
        self.assertEqual(result_uri.scheme, 'https')
        self.assertEqual(result_uri.hostname, 'test-domain.com')
        self.assertEqual(result_uri.port, 1234)
        self.assertEqual(result_uri.is_https, True)
        

    def get_client(self):
        client = base.BaseYarnAPI()
        client.service_uri = base.Uri('example.com:80')
        client.timeout = 0
        client.auth = None
        client.verify = True
        return client
