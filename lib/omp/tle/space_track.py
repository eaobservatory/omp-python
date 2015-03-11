# Copyright (C) 2014 Science and Technology Facilities Council.
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
"""Builds and checks ids, builds api request,
   sends request and returns list of tles
   """

import logging
from requests import session
from time import sleep

import omp.siteconfig as siteconfig

logger = logging.getLogger(__name__)


class SpaceTrack(object):
    """space-track.org API"""
    def __init__(self, tletype="NORAD"):
        self.tletype = tletype
        self.id_list = []

        self.max_request = 20
        self.request_delay = 5

    def add_id(self, catid):
        """Take NORAD Cat ID and adds it to the list."""
        if type(catid) is not str:
            catid = str(catid)
        if len(catid) > 5:
            try:
                raise ValueError("NORAD catid requires 5 or fewer digits.")
            except ValueError:
                print "Not good."
                raise
            return
        self.id_list.append(catid)

    def _build_request(self, ids):
        """Build a request URL.

        Returns the request URL as a string.
        """

        #Turns all ids into integers to strip spurious preceding 0's
        temp_set = set([int(r) for r in ids])
        #Sort back into a list.
        temp_list = sorted(list(temp_set))
        temp_list = [str(r) for r in temp_list]
        #comma separates
        temp_str = ",".join(temp_list)
        #Currently just what we need. But this could be parameterized.
        return ("https://www.space-track.org/basicspacedata/query/class/tle_latest/ORDINAL/1/NORAD_CAT_ID/" +
                     temp_str + "/orderby/EPOCH desc/format/tle")

    def send_request(self):
        """Send current request.

        Returns a list of lines.
        """

        url = "https://www.space-track.org/ajaxauth/login"
        cfg = siteconfig.get_omp_siteconfig()
        user = cfg.get('spacetrack', 'user')
        password = cfg.get('spacetrack', 'password')
        idpass = {'identity': user, 'password': password}
        result = []
        ids = self.id_list
        with session() as ss:
            r = ss.post(url, data=idpass)

            while ids:
                r = ss.get(self._build_request(ids[0:self.max_request]))
                result.extend(r.text.splitlines())

                ids = ids[self.max_request:]
                if ids:
                    logger.debug(
                        'Sleeping for %i seconds between space-track requests',
                        self.request_delay)
                    sleep(self.request_delay)

        return result
