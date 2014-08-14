"""parseTLE.py"""

#from degrees to radians
from math import radians
import datetime

class TLEParser(object):
	"""TLEParser"""
	def __init__(self):
		self.tle = {"NORAD": "",
					"Class": "",
					"Intl Desig": "",
					"Epoch": "",
					"First D": "",
					"Second D": "",
					"Bstar": "",
					"ElSet Type": "",
					"Element Num": "",
					"Inclination": "",
					"RA A Node": "",
					"E": "",
					"Perigee": "",
					"Mean Anomoly": "",
					"Mean Motion": "",
					"Rev at Epoch": ""}

	def convert_epoch(self, astro):
		astro = astro.strip()
		year = "20" + astro[:2]
		day = astro[2:astro.find(".")]
		tday = datetime.datetime.strptime(year + " " + day, '%Y %j')
		eday = datetime.datetime.strptime("1970 1", '%Y %j')
		days = (tday - eday).days
		return (float(astro[astro.find("."):]) + days) * 24 * 3600

	def export_tle_omp(self, tle):
		elements = {}
		elements["target"] = tle["NORAD"]
		elements["el1"] = tle["Epoch"]
		elements["el2"] = tle["Bstar"]
		elements["el3"] = tle["Inclination"]
		elements["el4"] = tle["RA A Node"]
		elements["el5"] = tle["E"]
		elements["el6"] = tle["Perigee"]
		elements["el7"] = tle["Mean Anomoly"]
		elements["el8"] = tle["Mean Motion"]
		return elements

	def write_tle(self, tle, ofile=None, obuffer=None):
		if ofile is not None:
			pass
		elif obuffer is not None:
			pass
		else:
			#raise exception
			return

	def parse_tle(self, line1, line2):
		tle = self.tle.copy()
		tle["NORAD"] = line1[2:7]
		tle["Class"] = line1[7]
		tle["Intl Desig"] = line1[9:17]
		tle["Epoch"] = self.convert_epoch(line1[18:32])
		tle["First D"] = line1[33:43]
		tle["Second D"] = line1[44:52]
		bstar = line1[53:61]
		if bstar.find("-") == 0:
			two_temp = bstar[1:].split("-")
			bstar = float("-0." + int(two_temp[1])*"0" + two_temp[0])
		else:
			two_temp = bstar.split("-")
			bstar = float("0." + int(two_temp[1])*"0" + two_temp[0])
		tle["Bstar"] = bstar
		tle["ElSet Type"] = line1[62]
		tle["Element Num"] = line1[64:68]
		tle["Inclination"] = radians(float(line2[8:16]))
		tle["RA A Node"] = radians(float(line2[17:25]))
		tle["E"] = float("." + line2[26:33])
		tle["Perigee"] = radians(float(line2[34:42]))
		tle["Mean Anomoly"] = radians(float(line2[43:51]))
		tle["Mean Motion"] = float(line2[52:63])
		tle["Rev at Epoch"] = line2[63:68]
		return tle
