#tle_omp.py

import Sybase
import omp.siteconfig as siteconfig

class TLE_OMP(object):
	"""Opens connection to omp database and allows tles to be submitted.
	   Defaults to devomp
	"""
	def __init__(self, omp="devomp"):
		user, password = self.enter_omp()
		self.db = Sybase.connect('SYB_JAC', user, password, omp)
		self.cursor = self.db.cursor()

	def submit_tle(self, tle):
		"""Takes tle and submits it into omp db"""
		self.cursor.execute("""
				INSERT INTO omptle
				(target, el1, el2, el3, el4, el5, el6, el7, el8)
				VALUES
				(@target, @el1, @el2, @el3, @el4, @el5, @el6, @el7, @el8)
						   """,
						   {
						   	'@target': tle["target"],
						   	'@el1': tle["el1"],
						   	'@el2': tle["el2"],
						   	'@el3': tle["el3"],
						   	'@el4': tle["el4"],
						   	'@el5': tle["el5"],
						   	'@el6': tle["el6"],
						   	'@el7': tle["el7"],
						   	'@el8': tle["el8"]
						   })
		self.db.commit()

	def retrieve_ids(self):
		"""Finds all auto update tles"""
		self.cursor.execute("SELECT target FROM ompobs WHERE coordstype=\"AUTO-TLE\"")
		return [r[0] for r in self.cursor.fetchall()]

	def update_tle_ompobs(self, tle):
		"""Places elements in omp."""
		self.cursor.execute("""
				UPDATE ompobs SET
				el1=@el1, el2=@el2, el3=@el3, el4=@el4,
				el5=@el5, el6=@el6, el7=@el7, el8=@el8
				WHERE target=@target
						   """,
						   {
						   	'@el1': tle["el1"],
						   	'@el2': tle["el2"],
						   	'@el3': tle["el3"],
						   	'@el4': tle["el4"],
						   	'@el5': tle["el5"],
						   	'@el6': tle["el6"],
						   	'@el7': tle["el7"],
						   	'@el8': tle["el8"],
						   	'@target': tle["target"]
						   })
		self.db.commit()

	def enter_omp(self):
		"""Finds and enters correct data to get in db"""
		cfg = siteconfig.get_omp_siteconfig()
		return cfg.get('database', 'user'), cfg.get('database', 'password')