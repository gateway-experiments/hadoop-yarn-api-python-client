# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import requests

from .errors import APIError, ConfigurationError


class Response(object):
    def __init__(self, response):
        self.data = response.json()


class BaseYarnAPI(object):
    __logger = None
    response_class = Response

    def __init__(self, address=None, port=None, timeout=None, kerberos_enabled=None):
        self.address, self.port, self.timeout, self.kerberos_enabled = address, port, timeout, kerberos_enabled

    def _validate_configuration(self):
        if self.address is None:
            raise ConfigurationError('API address is not set')
        elif self.port is None:
            raise ConfigurationError('API port is not set')

    def request(self, api_path, method='GET', **kwargs):
        api_endpoint = 'http://{}:{}{}'.format(self.address, self.port, api_path)

        self.logger.info('API Endpoint {}'.format(api_endpoint))

        self._validate_configuration()

        if method == 'GET':
            headers = None
        else:
            headers = {"Content-Type": "application/json"}

        response = None
        if self.kerberos_enabled:
            from requests_kerberos import HTTPKerberosAuth
            response = requests.request(method=method, url=api_endpoint, auth=HTTPKerberosAuth(), headers=headers, **kwargs)
        else:
            response = requests.request(method=method, url=api_endpoint, headers=headers, **kwargs)

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
