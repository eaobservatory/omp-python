#tle_omp.py

import Sybase

class TLE_OMP(object):
	"""Opens connection to omp database and allows tles to be submitted."""
	def __init__(self):
		user, password = self.enter_omp()
		self.db = Sybase.connect('SYB_JAC', user, password, 'devomp')
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

	def place_tle_elements(self, tle):
		"""Places elements in omp."""
		self.cursor.execute("""
				UPDATE ompobs SET
				el1=@el1 AND el2=@el2 AND el3=@el3 AND el4=@el4 AND
				el5=@el5 AND el6=@el6 AND el7=@el7 AND el8=@el8
				WHERE target=NORAD@target
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
		flag = 0
		user = ""
		password = ""
		with open("/jac_sw/etc/omp1site.cfg", 'r') as sfile:
			for line in sfile:
				if '[database]' in line:
					flag = 1
				elif flag == 1 and 'user' in line:
					user = line.split('=')[1].strip()
				elif flag == 1 and 'password' in line:
					password = line.split('=')[1].strip()
					flag = 0
					break
		return user, password