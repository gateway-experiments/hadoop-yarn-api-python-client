# -*- coding: utf-8 -*-


class APIError(Exception):
    pass


class ConfigurationError(APIError):
    pass


class IllegalArgumentError(APIError):
    pass
