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

import logging

from omp.siteconfig import get_omp_siteconfig
from omp.db.backend.sybase import OMPSybaseLock

logger = logging.getLogger(__name__)


class TLEDB(object):
    """Opens connection to omp database and allows tles to be submitted.
       Defaults to devomp
    """
    def __init__(self, **kwargs):
        cfg = get_omp_siteconfig()
        user = cfg.get('database', 'user')
        password = cfg.get('database', 'password')

        logger.debug('Connecting to OMP, user:%s', user)
        self.db = OMPSybaseLock(
            server='SYB_JAC',
            user=user,
            password=password,
            **kwargs)

    def submit_tle(self, tle):
        """Takes tle and submits it into omp db"""
        with self.db.transaction(read_write=True) as cursor:
            logger.debug('Deleting old omptle row for "%s"', tle['target'])
            cursor.execute("DELETE FROM omp..omptle WHERE target=@target",
                           {
                               '@target': tle["target"]
                           }
                           )

            logger.debug('Inserting new omptle row: %s', repr(tle))
            cursor.execute("""
                            INSERT INTO omp..omptle
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

    def retrieve_ids(self):
        """Finds all auto update tles"""
        with self.db.transaction() as cursor:
            logger.debug('Retrieving list of distinct AUTO-TLE targets from ompobs')
            cursor.execute("SELECT DISTINCT target FROM omp..ompobs WHERE coordstype=\"AUTO-TLE\"")
            rows = cursor.fetchall()

        return [r[0] for r in rows]

    def update_tle_ompobs(self, tle):
        """Places elements in omp."""

        with self.db.transaction(read_write=True) as cursor:
            logger.debug('Updating ompobs with: %s', repr(tle))
            cursor.execute("""
                            UPDATE omp..ompobs SET
                            el1=@el1, el2=@el2, el3=@el3, el4=@el4,
                            el5=@el5, el6=@el6, el7=@el7, el8=@el8
                            WHERE coordstype="AUTO-TLE" AND target=@target
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
