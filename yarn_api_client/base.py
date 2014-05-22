# -*- coding: utf-8 -*-
from httplib import HTTPConnection, OK
import json
import logging
import urllib

from .errors import APIError, ConfigurationError


class Response(object):
    def __init__(self, http_response):
        self.data = json.load(http_response)


class BaseYarnAPI(object):
    response_class = Response

    def request(self, api_path, **query_args):
        params = urllib.urlencode(query_args)
        if params:
            path = api_path + '?' + params
        else:
            path = api_path

        self.logger.info(u'Request http://%s:%s%s', self.address, self.port, path)
        self.http_conn.request('GET', path)

        response = self.http_conn.getresponse()

        if response.status == OK:
            return self.response_class(response)
        else:
            msg = u'Response finished with status: %s' % response.status
            raise APIError(msg)

    __http_conn = None
    @property
    def http_conn(self):
        if self.__http_conn is None:
            if self.address is None:
                raise ConfigurationError(u'API address is not set')
            elif self.port is None:
                raise ConfigurationError(u'API port is not set')
            self.__http_conn = HTTPConnection(self.address, self.port,
                                              timeout=self.timeout)

        return self.__http_conn

    __logger = None
    @property
    def logger(self):
        if self.__logger is None:
            self.__logger = logging.getLogger(self.__module__)
        return self.__logger
