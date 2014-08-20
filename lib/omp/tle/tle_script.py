#tle_script.py

import spaceTracker
import parseTLE
import tle_omp

import sys

strack = spaceTracker.SpaceTrack()
parse = parseTLE.TLEParser()
omp = tle_omp.TLE_OMP()

errors = []

ids = omp.retrieve_ids()

for cat_id in ids:
	if not cat_id.find("NORAD"):
		errors.append(cat_id)
		continue
	cat_id = cat_id[5:]
	strack.add_id(cat_id)

strack.build_request()

tles = strack.send_request()

line1 = ""
line2 = ""
flag = 0

for tle in tles:
	if flag == 1:
		line2 = tle
		parsed = parse.parse_tle(line1, line2)
		ex_tle = parse.export_tle(parsed)
		submit.submit_tle(ex_tle)
		flag = 0
	elif flag == 0:
		line1 = tle
		flag = 1

if len(errors) != 0:
	err_str = "Errors encountered" + ", ".join(errors)
	sys.exit(err_str)
