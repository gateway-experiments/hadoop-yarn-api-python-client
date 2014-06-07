# -*- coding: utf-8 -*-
from unittest import TestCase

from yarn_api_client import constants


class ConstantsTestCase(TestCase):
    def test_stats_len(self):
        self.assertEqual(8, len(constants.YarnApplicationState))
        self.assertEqual(6, len(constants.ApplicationState))
        self.assertEqual(4, len(constants.FinalApplicationStatus))
        self.assertEqual(14, len(constants.JobStateInternal))
