# -*- coding: utf-8 -*-
from tests import TestCase

import yarn_api_client.main as m


class MainTestCase(TestCase):
    def test_get_parser(self):
        m.get_parser()
