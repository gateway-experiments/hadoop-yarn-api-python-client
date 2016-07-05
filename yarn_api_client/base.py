# -*- coding: utf-8 -*-
from __future__ import unicode_literals
try:
    from httplib import HTTPConnection, OK
except ImportError:
    from http.client import HTTPConnection, OK
import json
import logging
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

from .errors import APIError, ConfigurationError


class Response(object):
    def __init__(self, http_response):
        self.data = json.load(http_response)


class BaseYarnAPI(object):
    response_class = Response

    def request(self, api_path, **query_args):
        params = urlencode(query_args)
        if params:
            path = api_path + '?' + params
        else:
            path = api_path

        self.logger.info('Request http://%s:%s%s', self.address, self.port, path)
        
        http_conn = self.http_conn
        http_conn.request('GET', path)
        response = http_conn.getresponse()

        if response.status == OK:
            return self.response_class(response)
        else:
            explanation = response.read()
            msg = 'Response finished with status: %s. Details: %s' % (response.status, explanation)
            raise APIError(msg)

    def construct_parameters(self, arguments):
        params = dict((key, value) for key, value in arguments if value is not None)
        return params

    @property
    def http_conn(self):
        if self.address is None:
            raise ConfigurationError('API address is not set')
        elif self.port is None:
            raise ConfigurationError('API port is not set')
        return HTTPConnection(self.address, self.port, timeout=self.timeout)

    __logger = None
    @property
    def logger(self):
        if self.__logger is None:
            self.__logger = logging.getLogger(self.__module__)
        return self.__logger
