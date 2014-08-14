#submit_omp.py

import Sybase
from contextlib import closing

class SubmitOMP(object):
	"""Opens connection to omp database and allows tles to be submitted."""
	def __init__(self):
		user, password = self.enter_omp()
		self.db = Sybase.connect('SYB_JAC', user, password, 'devomp')

	def submit_tle(self, tle):
		"""Takes tle and submits it into omp db"""
		with closing(self.db.cursor()) as cursor:
			cursor.execute("""
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