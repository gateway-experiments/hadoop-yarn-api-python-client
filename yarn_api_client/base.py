# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import requests

from .errors import APIError, ConfigurationError

try:
    from urlparse import urlparse, urlunparse
except ImportError:
    from urllib.parse import urlparse, urlunparse


class Response(object):
    def __init__(self, response):
        self.data = response.json()


class Uri(object):
    def __init__(self, service_endpoint):
        service_uri = urlparse(service_endpoint)
        self.scheme = service_uri.scheme or 'http'
        self.hostname = service_uri.hostname or service_uri.path
        self.port = service_uri.port
        self.is_https = service_uri.scheme == 'https' or False

    def to_url(self, api_path=None):
        path = api_path or ''
        if self.port:
            result_url = urlunparse((self.scheme, self.hostname + ":" + str(self.port), path, None, None, None))
        else:
            result_url = urlunparse((self.scheme, self.hostname, path, None, None, None))

        return result_url


class BaseYarnAPI(object):
    __logger = None
    response_class = Response

    def __init__(self, service_endpoint=None, timeout=None, auth=None, verify=True):
        self.timeout = timeout

        if service_endpoint:
            self.service_uri = Uri(service_endpoint)
        else:
            self.service_uri = None

        self.session = requests.Session()
        self.session.auth = auth
        self.session.verify = verify

    def _validate_configuration(self):
        if not self.service_uri:
            raise ConfigurationError('API endpoint is not set')

    def request(self, api_path, method='GET', **kwargs):
        self._validate_configuration()
        api_endpoint = self.service_uri.to_url(api_path)

        self.logger.info('API Endpoint {}'.format(api_endpoint))

        if method == 'GET':
            headers = {}
        else:
            headers = {"Content-Type": "application/json"}

        if 'headers' in kwargs and kwargs['headers']:
            headers.update(kwargs['headers'])

        response = self.session.request(method=method, url=api_endpoint, headers=headers, timeout=self.timeout, **kwargs)

        if response.status_code in (200, 202):
            return self.response_class(response)
        else:
            msg = 'Response finished with status: %s. Details: %s' % (response.status_code, response.text)
            raise APIError(msg)

    def construct_parameters(self, arguments):
        params = dict((key, value) for key, value in arguments if value is not None)
        return params

    @property
    def logger(self):
        if self.__logger is None:
            self.__logger = logging.getLogger(self.__module__)
        return self.__logger
