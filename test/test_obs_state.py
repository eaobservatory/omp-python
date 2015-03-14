# Copyright (C) 2015 EAO
#
# TODO: insert license here

from unittest import TestCase

from omp.error import OMPError
from omp.obs.state import OMPState


class TestState(TestCase):
    def test_get_name(self):
        self.assertEqual(OMPState.get_name(OMPState.GOOD), 'Good')
        self.assertEqual(OMPState.get_name(OMPState.QUESTIONABLE), 'Questionable')
        self.assertEqual(OMPState.get_name(OMPState.BAD), 'Bad')
        self.assertEqual(OMPState.get_name(OMPState.REJECTED), 'Rejected')
        self.assertEqual(OMPState.get_name(OMPState.JUNK), 'Junk')

        with self.assertRaises(OMPError):
            OMPState.get_name(999)

    def test_is_valid(self):
        self.assertFalse(OMPState.is_valid(-1))
        self.assertTrue(OMPState.is_valid(0))
        self.assertTrue(OMPState.is_valid(4))
        self.assertFalse(OMPState.is_valid(5))
