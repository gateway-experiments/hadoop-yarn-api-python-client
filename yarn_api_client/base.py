# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import logging
import requests

from requests_kerberos import HTTPKerberosAuth
from .errors import APIError, ConfigurationError


class Response(object):
    def __init__(self, response):
        self.data = response.json()


class BaseYarnAPI(object):
    __logger = None
    response_class = Response

    def _validate_configuration(self):
        if self.address is None:
            raise ConfigurationError('API address is not set')
        elif self.port is None:
            raise ConfigurationError('API port is not set')

    def request(self, api_path, **query_args):
        params = query_args
        api_endpoint = 'http://{}:{}{}'.format(self.address, self.port, api_path)

        self.logger.info('API Endpoint {}'.format(api_endpoint))

        self._validate_configuration()

        response = None
        if self.kerberos_enabled:
            response = requests.get(api_endpoint, params, auth=HTTPKerberosAuth())
        else:
            response = requests.get(api_endpoint, params)

        if response.status_code == requests.codes.ok:
            return self.response_class(response)
        else:
            msg = 'Response finished with status: %s. Details: %s' % (response.status_code, response.text)
            raise APIError(msg)

    def update(self, api_path, data):
        api_endpoint = 'http://{}:{}{}'.format(self.address, self.port, api_path)

        self.logger.info('API Endpoint {}'.format(api_endpoint))

        self._validate_configuration()

        response = None
        if self.kerberos_enabled:
            response = requests.put(api_endpoint, data=data, auth=HTTPKerberosAuth())
        else:
            response = requests.put(api_endpoint, data=data)

        if response.status_code == requests.codes.ok:
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
