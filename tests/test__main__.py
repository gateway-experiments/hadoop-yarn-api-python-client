# -*- coding: utf-8 -*-
from mock import patch, PropertyMock
from unittest import TestCase

import yarn_api_client.__main__ as m


class MainTestCase(TestCase):
    def test_get_parser(self):
        m.get_parser()
