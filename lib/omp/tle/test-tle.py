#test-tle.py
import unittest
import random
import spaceTracker


class TestSpaceTrack(unittest.TestCase):

	def setUp(self):
		self.st = spaceTracker.SpaceTrack()

	def tearDown(self):
		self.st = None

	def test_add_id_exception(self):
		"""
		Any integer more than 5 digits should raise an exception
		"""
		self.assertRaises(ValueError, self.st.add_id, '255678')

	def test_add_id_short(self):
		"""Test random numbers under 5 digits."""
		for i in range(1000):
			rnd = random.randint(1,9999)
			rnd_str = str(rnd)
			self.st.add_id(rnd_str)
			self.assertEqual(int(self.st.id_list[-1]), rnd)

	def test_add_id_five(self):
		"""Test random five digit numbers"""
		for i in range(1000):
			rnd = random.randint(10000,99999)
			rnd_str = str(rnd)
			self.st.add_id(rnd_str)
			self.assertEqual(int(self.st.id_list[-1]), rnd)


suite = unittest.TestLoader().loadTestsFromTestCase(TestSpaceTrack)
unittest.TextTestRunner(verbosity=3).run(suite)