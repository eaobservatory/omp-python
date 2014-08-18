#tle_script.py

import spaceTracker
import parseTLE
import submit_omp

import sys

strack = spaceTracker.SpaceTrack()
parse = parseTLE.TLEParser()
submit = submit_omp.SubmitOMP()

if len(sys.argv) < 2:
	print "Please enter catolog ids, comma separated."
	sys.exit()

ids = sys.argv[1].split(",")

for cat_id in ids:
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
