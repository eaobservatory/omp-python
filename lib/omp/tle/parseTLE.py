"""parseTLE.py"""

from codecs import latin_1_encode
import datetime
import logging
#from degrees to radians
from math import radians

logger = logging.getLogger(__name__)

class TLEParser(object):
	"""TLEParser"""
	def __init__(self, tletype="NORAD"):
		self.tletype = tletype
		self.tle = {tletype: "",
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
		elements["target"] = tle["NORAD"] #self.tletype]
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
		logger.debug('Parsing TLE, line 1: {0}'.format(line1))
		logger.debug('Parsing TLE, line 2: {0}'.format(line2))

		if len(line1) < 62 or len(line2) < 69:
			return None
		tle = self.tle.copy()
		tle[self.tletype] = self.tletype + latin_1_encode(line1[2:7])[0]
		tle["Class"] = line1[7]
		tle["Intl Desig"] = line1[9:17]
		tle["Epoch"] = self.convert_epoch(line1[18:32])
		tle["First D"] = line1[33:43]
		tle["Second D"] = line1[44:52]
		tle["Bstar"] = self._parse_decimal_rhs(line1[53:61])
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

	def _parse_decimal_rhs(self, decimal):
		"""Routine to parse TLE-style right hand sides of
		truncated decimals.  (i.e. the bit after the decimal
		point)
		"""
		if decimal.startswith('-'):
			decimal = decimal[1:]
			sign = -1.0
		else:
			sign = 1.0

		if '-' in decimal:
			decimal = 'E-'.join(decimal.split('-'))
		elif '+' in decimal:
			decimal = 'E+'.join(decimal.split('+'))

		return sign * float('0.' + decimal.strip())
