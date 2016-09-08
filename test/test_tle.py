# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2016 East Asian Observatory.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import unittest
import random
import time
from omp.tle.space_track import SpaceTrack
from omp.tle.parse import TLEParser
from omp.db.part.tle import TLEDB


class TestSpaceTrack(unittest.TestCase):
    def setUp(self):
        self.st = SpaceTrack()

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
            rnd = random.randint(1, 9999)
            rnd_str = str(rnd)
            self.st.add_id(rnd_str)
            self.assertEqual(int(self.st.id_list[-1]), rnd)

    def test_add_id_five(self):
        """Test random five digit numbers"""
        for i in range(1000):
            rnd = random.randint(10000, 99999)
            rnd_str = str(rnd)
            self.st.add_id(rnd_str)
            self.assertEqual(int(self.st.id_list[-1]), rnd)

    def test_add_id_numeral(self):
        """Test random integers"""
        for i in range(1000):
            rnd = random.randint(1, 99999)
            self.st.add_id(rnd)
            self.assertEqual(int(self.st.id_list[-1]), rnd)

    def test_build_request(self):
        """Too simple? build request test."""
        _list = [345, 20, 45034, 2401, 4242]
        for each in _list:
            self.st.add_id(each)
        _list.sort()
        comma_str = ",".join([str(r) for r in _list])
        self.assertEqual(
            self.st._build_request(self.st.id_list),
            "https://www.space-track.org/basicspacedata/query/class/tle_latest/ORDINAL/1/NORAD_CAT_ID/20,345,2401,4242,45034/orderby/EPOCH desc/format/tle")


class TestTLEParse(unittest.TestCase):
    def setUp(self):
        self.parse = TLEParser()

    def tearDown(self):
        self.parse = None

    def test_parse_decimal_rhs(self):
        self.assertEqual(self.parse._parse_decimal_rhs(' 00000+0'), 0.0)
        self.assertEqual(self.parse._parse_decimal_rhs(' 00000-0'), 0.0)
        self.assertEqual(self.parse._parse_decimal_rhs(' 10000+0'), 0.1)
        self.assertEqual(self.parse._parse_decimal_rhs(' 10000-0'), 0.1)
        self.assertEqual(self.parse._parse_decimal_rhs(' 10000+1'), 1.0)
        self.assertEqual(self.parse._parse_decimal_rhs(' 10000-1'), 0.01)
        self.assertEqual(self.parse._parse_decimal_rhs('-00000+0'), -0.0)
        self.assertEqual(self.parse._parse_decimal_rhs('-00000-0'), -0.0)
        self.assertEqual(self.parse._parse_decimal_rhs('-10000+0'), -0.1)
        self.assertEqual(self.parse._parse_decimal_rhs('-10000-0'), -0.1)
        self.assertEqual(self.parse._parse_decimal_rhs('-10000+1'), -1.0)
        self.assertEqual(self.parse._parse_decimal_rhs('-10000-1'), -0.01)

    def test_parse_tle(self):
        """print parse"""
        ans = self.parse.parse_tle("1 25544U 98067A   14206.52997318 -.00005757  00000-0 -91404-4 0  7690",
                                   "2 25544 051.6472 269.5323 0006361 286.1580 210.2768 15.50427728897273")
        self.assertEqual(ans, {'E': 0.0006361, 'RA A Node': 4.7042260754731124,
                               'First D': '-.00005757', 'Epoch': 1406292189.682752,
                               'Intl Desig': '98067A  ', 'Bstar': -0.000091404,
                               'Inclination': 0.9014136894360153, 'Mean Motion': 15.50427728,
                               'NORAD': 'NORAD25544', 'Perigee': 4.994399280921934,
                               'ElSet Type': '0', 'Element Num': ' 769',
                               'Second D': ' 00000-0', 'Rev at Epoch': '89727',
                               'Class': 'U', 'Mean Anomoly': 3.6700225005576126})

        # Test correcting badly formatted identifiers.
        ans = self.parse.parse_tle("1  5544U 98067A   14206.52997318 -.00005757  00000-0 -91404-4 0  7690",
                                   "2  5544 051.6472 269.5323 0006361 286.1580 210.2768 15.50427728897273")
        self.assertEqual(ans['NORAD'], 'NORAD05544')

        with self.assertRaisesRegexp(ValueError, 'invalid identifier NOT_N'):
            self.parse.parse_tle("1 NOT_NU 98067A   14206.52997318 -.00005757  00000-0 -91404-4 0  7690",
                                 "2 NOT_N 051.6472 269.5323 0006361 286.1580 210.2768 15.50427728897273")

        with self.assertRaisesRegexp(ValueError, 'identifier -1234 out of range'):
            self.parse.parse_tle("1 -1234U 98067A   14206.52997318 -.00005757  00000-0 -91404-4 0  7690",
                                 "2 -1234 051.6472 269.5323 0006361 286.1580 210.2768 15.50427728897273")

    def test_convert_epoch(self):
        """Test Epoch converter"""
        epoch = "14099.5"
        self.assertEqual(1397044800.0, self.parse.convert_epoch(epoch))

    def test_export_tle_omp(self):
        ans = self.parse.parse_tle("1 25544U 98067A   14206.52997318 -.00005757  00000-0 -91404-4 0  7690",
                                   "2 25544 051.6472 269.5323 0006361 286.1580 210.2768 15.50427728897273")
        export = self.parse.export_tle_omp(ans)
        self.assertEqual(export, {'target': 'NORAD25544', 'el8': 15.50427728,
                                  'el2': -0.000091404, 'el3': 0.9014136894360153,
                                  'el1': 1406292189.682752, 'el6': 4.994399280921934,
                                  'el7': 3.6700225005576126, 'el4': 4.7042260754731124,
                                  'el5': 0.0006361})


class TestTLEOMP(unittest.TestCase):
    def setUp(self):
        self.subomp = TLEDB(read_only=True)

    def tearDown(self):
        self.subomp = None

    def test_submit_tle(self):
        # self.subomp.submit_tle({'target': 'NORAD25544', 'el8': 15.50427728,
        #                          'el2': -0.000091404, 'el3': 0.9014136894360153,
        #                          'el1': 1406292189.682752, 'el6': 4.994399280921934,
        #                          'el7': 3.6700225005576126, 'el4': 4.7042260754731124,
        #                          'el5': 0.0006361})
        pass

    def test_update_tle_ompobs(self):
        pass

    def test_retrieve_ids(self):
        # self.assertEqual(["NORAD39504"], self.subomp.retrieve_ids())
        pass
