# Copyright (C) 2014-2015 Science and Technology Facilities Council.
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

from __future__ import print_function, division, absolute_import

from collections import namedtuple, OrderedDict
from datetime import datetime
from keyword import iskeyword

from pytz import UTC

from omp.db.backend.sybase import OMPSybaseLock
from omp.error import OMPDBError


class OMPDB:
    """OMP and JCMT database access class.
    """

    CommonInfo = None

    def __init__(self, **kwargs):
        """Construct new OMP and JCMT database object.

        Connects to the JAC Sybase server.

        """

        self.db = OMPSybaseLock(**kwargs)

    def get_obsid_common(self, obsid):
        """Retrieve information for a given obsid from the COMMON table.
        """

        with self.db.transaction() as c:
            c.execute(
                'SELECT * FROM jcmt..COMMON WHERE obsid=@o',
                {'@o': obsid})

            rows = c.fetchall()
            cols = c.description

        if not rows:
            return None

        elif len(rows) > 1:
            raise OMPDBError('multiple COMMON results for one obsid')

        if self.CommonInfo is None:
            self.CommonInfo = namedtuple(
                'CommonInfo',
                ['{0}_'.format(x[0]) if iskeyword(x[0]) else x[0]
                 for x in cols])

        return self.CommonInfo(*rows[0])

    def get_obsid_status(self, obsid):
        """Retrieve the last comment status for a given obsid.

        Returns None if no status was found.
        """

        with self.db.transaction() as c:
            c.execute(
                'SELECT commentstatus FROM omp..ompobslog '
                'WHERE obslogid = '
                '(SELECT MAX(obslogid) FROM omp..ompobslog '
                'WHERE obsid=@o AND obsactive=1)',
                {'@o': obsid})

            rows = c.fetchall()

        if not rows:
            return None

        if len(rows) > 1:
            raise OMPDBError('multiple status results for one obsid')

        return rows[0][0]

    def find_obs_for_ingestion(self, utdate_start, utdate_end=None,
                               no_status_check=False, no_transfer_check=False):
        """Find (raw) observations which are due for ingestion into CAOM-2.

        This method searches for observations matching these criteria:

            1. utdate within the given range
            2. date_obs at least 4 hours ago
            3. last_caom_mod NULL, older than last_modified or older than
               last comment
            4. no files still in the process of being transferred

        Arguments:
            utdate_start: start date (observation's UT date must be >= this)
                          as a "YYYYMMDD" integer.  Can also be None to remove
                          the restriction, but this is not advisable for the
                          start date.
            utdate_end:   similar to utdate_end but for the end of the date
                          range (default: None).
            no_status_check: disable criterion 3, and instead only look for
                             observations with NULL last_caom_mod
            no_transfer_check: disable criterion 4

        Returns:
            A list of OBSID strings.
        """

        where = []
        args = {}

        # Consider date range limits.
        if utdate_start is not None:
            args['@us'] = utdate_start
            where.append('(utdate >= @us)')
        if utdate_end is not None:
            args['@ue'] = utdate_end
            where.append('(utdate <= @ue)')

        # Check the observation is finished.  (Started >= 4 hours ago.)
        where.append('(DATEDIFF(hh, date_obs, GETUTCDATE()) >= 4)')

        # Look for last_caom_mod NULL, older than last_modified
        # or (optionally) comment newer than last_caom_mod.
        status_condition = [
            '(last_caom_mod IS NULL)',
            '(last_modified > last_caom_mod)',
        ]
        if not no_status_check:
            status_condition.append(
                            '(last_caom_mod < (SELECT MAX(commentdate)'
                                ' FROM omp..ompobslog AS o'
                                ' WHERE o.obsid=c.obsid))')
        where.append('(' + ' OR '.join(status_condition) + ')')

        # Check that all files have been transferred.
        if not no_transfer_check:
            where.append('(SELECT COUNT(*) FROM jcmt..FILES AS f'
                            ' JOIN jcmt..transfer AS t'
                            ' ON f.file_id=t.file_id'
                            ' WHERE f.obsid=c.obsid'
                                ' AND t.status NOT IN ("t", "d", "D", "z"))'
                            ' = 0')

        query = 'SELECT obsid FROM jcmt..COMMON AS c WHERE ' + ' AND '.join(where)
        result = []

        with self.db.transaction() as c:
            c.execute(query, args)

            while True:
                row = c.fetchone()
                if row is None:
                    break

                result.append(row[0])

        return result

    def set_last_caom_mod(self, obsid, set_null=False):
        """Set the "COMMON.last_caom_mod" column to the current date
        and time for the given observation.

        This is to be used to mark an observation as successfully ingested
        into CAOM-2 (raw data only).

        If the set_null option is given then last_caom_mod is nulled rather
        than being set to the current date and time.
        """

        query = 'UPDATE jcmt..COMMON SET last_caom_mod = ' + \
            ('NULL' if set_null else 'GETUTCDATE()') + \
            ' WHERE obsid=@o'
        args = {'@o': obsid}

        with self.db.transaction(read_write=True) as c:
            c.execute(query, args)

            # Check that exactly one row was updated.
            # TODO: reinstate this check if/when we migrate to a
            # database where rowcount works.
            # if c.rowcount == 0:
            #     raise NoRowsError('COMMON', query, args)
            # elif c.rowcount > 1:
            #     raise ExcessRowsError('COMMON', query, args)


    def find_obs_by_date(self, utstart, utend, instrument=None):
        """
        Find observations from jcmt.COMMON in the OMP from date.

        This takes a start utdate and end utdate (can be the same to
        limit search to one day) and finds all observation in common.

        Can optionally be limited by instrument name (based on
        INSTRUME column). Instrument name is not case sensitive.

        Args:
            utstart (int): start date (inclusive) in YYYYMMDD format
            utend (int):  end date (inclusive) in YYYYMMDD format
            instrument (str): optional, limit results by this instrume name.

        Returns:
            list of str: All obsids found that match the limits.

        """

        query = ('SELECT obsid FROM jcmt..COMMON WHERE utdate>=@s AND '
                 ' utdate <=@e ')
        args = {'@s': utstart, '@e': utend}

        if instrument:
            query += ' AND upper(instrume)=@i'
            args['@i'] = instrument.upper()

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)

            rows = c.fetchall()

        # Reformat output list.
        if rows:
            rows = [i[0] for i in rows]

        return rows


    def find_releasedates(self, utstart, utend, instrument=None, backend=None):
        """
        Find releasedates from COMMON from date & instrument.

        This takes a start utdate and end utdate (can be the same to
        limit search to one day) and finds all obsids and their releasedates
        from jcmt..COMMON. Instrument search is not case sensitive.

        Can optionally be limited by instrument name (based on INSTRUME column)

        Args:
            utstart (int): start date (inclusive) in YYYYMMDD format
            utend (int):  end date (inclusive) in YYYYMMDD format
            instrument (str): optional, limit results by this instrume name.
            backend (str): optional, limit results by this backend

        Returns:
            list of tuples: All obsids & releasedate pairs found that match the limits.

        """

        query = ('SELECT obsid, release_date FROM jcmt..COMMON WHERE utdate>=@s AND '
                 ' utdate <=@e ')
        args = {'@s': utstart, '@e': utend}

        if instrument:
            query += ' AND upper(instrume)=@i'
            args['@i'] = instrument.upper()
        if backend:
            query += ' AND upper(backend)=@b'
            args['@b'] = backend.upper()

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)

            rows = c.fetchall()

        return rows

    def get_observations_from_project(self, projectcode, utdatestart=None, utdateend=None, instrument=None):
        """Get information about a project's observations.

        This is designed for getting summary information for
        monitoring of jcmt large programs. It can be limited by date
        range or instrument.

        Parameters:

        projectcode (str): required, OMP project code.

        utdatestart (int,YYYYMMDD'): optional, limit to observations after this date (inc)
        utdateend (int, 'YYYYMMDD'): optional, limit to observations before this date (inc)
        instrument (str): optional, limit by instrument

        Return a dictionary of namedtuples, with the obsid as the key.

        """

        query = ("SELECT c.obsid, instrume, c.wvmtaust, c.wvmtauen, c.utdate, c.obsnum, c.object,"
                 " datediff(second, c.date_obs, c.date_end) as time, o.commentstatus, o.commenttext"
                 " FROM jcmt..COMMON as c join omp..ompobslog as o"
                 " ON  c.obsid=o.obsid"
                 " WHERE project=@p"
                 " AND obslogid in (SELECT MAX(obslogid) FROM omp..ompobslog GROUP BY obsid)")

        args = {'@p': str(projectcode).upper()}

        # Limit by instrument and date if requested.
        if instrument:
            query += ' AND upper(instrume)=@i'
            args['@i'] = str(instrument).upper()

        if utdatestart:
            query += ' AND utdate >= @s'
            args['@s'] = utdatestart

        if utdateend:
            query += ' AND utdate <= @e'
            args['@e'] = utdateend


        # Order by date.
        query += ' ORDER BY c.utdate ASC '

        projobsinfo = namedtuple('projobsinfo',
            'obsid instrument wvmtaust wvmtauen utdate obsnum object duration status commenttext')

        # Carry out query
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = OrderedDict( [ [i[0], projobsinfo(*i)] for i in rows] )
        return results

    def get_summary_obs_info(self, projectpattern):
        """Get summary of obs info for projects.

        Gets the number and duration of obsrvations per project, split
        up by omp status, weatherband and instrument.

        """
        projobsinfo = namedtuple('projobsinfo', 'project instrument band status number totaltime')

        query = ("SELECT t.project, t.instrume, t.band, t.commentstatus," \
                 "       count(*) as numobs, sum(t.duration) as totaltime " \
                 "FROM ( " \
                 "      SELECT c.project, c.instrume, datediff(second, c.date_obs, c.date_end) as duration," \
                 "             CASE WHEN o.commentstatus is NULL "\
                 "                  THEN 0 "\
                 "                  ELSE o.commentstatus "\
                 "             END AS commentstatus, "\
                 "             CASE WHEN (wvmtaust+wvmtauen)/2.0 between 0    and 0.05 then '1' "\
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.05 and 0.08 then '2' "\
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.08 and 0.12 then '3' "\
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.12 and 0.2  then '4' "\
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.2  and 100  then '5' "\
                 "                  ELSE 'unknown' "\
                 "             END AS band "\
                 "      FROM jcmt..COMMON AS c, omp..ompobslog AS o "\
                 "      WHERE c.obsid*=o.obsid AND project LIKE @p "\
                 "            AND o.obslogid IN (SELECT MAX(obslogid) FROM omp..ompobslog GROUP BY obsid) "\
                 "    ) t "\
                 "GROUP BY t.project, t.instrume, t.band, t.commentstatus "\
                 "ORDER BY t.project, t.instrume, t.band ASC, t.commentstatus ASC ")

        args = {'@p': projectpattern }

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [projobsinfo(*i) for i in rows]

        return results

    def get_summary_msb_info(self, projectpattern):
        """Get overview of the msbs waiting to be observed.

        Returns a list of namedtuples, each namedtuple represents the
        summary for one project that matches the projectpattern.

        """
        projmsbinfo = namedtuple('projmsbinfo', 'project uniqmsbs totalmsbs totaltime taumin taumax')

        query = ("SELECT o.projectid, count(*), sum(o.remaining), "\
                 "       sum(o.timeest*o.remaining), o.taumin, o.taumax "\
                 "FROM omp..ompmsb as o "\
                 "WHERE o.projectid LIKE @p AND o.remaining > 0 "\
                 "GROUP BY o.taumin, o.taumax, o.projectid "\
                 "ORDER BY o.projectid, o.taumin, o.taumax ")

        args = {'@p': projectpattern}

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [projmsbinfo(*i) for i in rows]

        return results

    def get_time_charged_project_info(self, projectcode):
        """
        Get time charged per day for a project.

        Returns list of namedtuples, ordered by date.
        """

        query = "SELECT date, timespent, confirmed from omp..omptimeacct WHERE projectid=@p ORDER BY date ASC"
        args = {'@p': projectcode}

        timeinfo = namedtuple('timeinfo', 'date timespent confirmed')

        # Carry out query
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [timeinfo(*i) for i in rows]
        return results



    def get_fault_summary(self, projectpattern):

        """
        Get all faults associated with  projects matching the projectpattern.

        projectpattern: string, needs to match projectids in a LIKE DB
        search.  e.g. projectpattern='M16AL%' would find all the 16A
        large programmes.

        Returns a list of namedtuples.

        """
        query = ("SELECT a.projectid, f.faultid, f.status, f.subject "\
                 "FROM omp..ompfaultassoc as a JOIN omp..ompfault as f "\
                 "ON a.faultid = f.faultid "\
                 "WHERE a.projectid LIKE @p")
        args = {'@p': projectpattern.lower()}
        faultinfo = namedtuple('faultinfo', 'project faultid status subject')
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [faultinfo(*i) for i in rows]
        return results

    def get_allocation_project(self, projectcode, like=None):
        """
        Get allocation info for a project.

        If like=True, then use a 'LIKE' match and get results for
        multiple projects.

        Return a dictionary of named tuples, with the projectcode as
        the key.

        """

        allocinfo = namedtuple('allocinfo', 'pi title semester allocated remaining pending taumin taumax')

        query = ("SELECT projectid, pi, title, semester, allocated, remaining, pending, taumin, taumax "
                 " FROM omp..ompproj")

        if like:
            query += ' WHERE projectid LIKE @p '
        else:
            query+='  WHERE projectid=@p '

        args={'@p': projectcode}

        # Carry out query
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()

            results = OrderedDict([[i[0], allocinfo(*i[1:])] for i in rows])

        return results

    def get_cadcusers_and_projects(self, telescope='JCMT'):
        """
        Get COI and PI cadcusernames for all projects.

        Excludes semester='MEGA' and semester='JAC'.

        Returns a list of namedtuples, giving the projectid, the cadc
        username and the capacity (i.e. COI or PI).

        """

        projectuser = namedtuple('projectuser', 'project cadcuser capacity')

        query=(
            "SELECT a.projectid, b.cadcuser, a.capacity "\
            "FROM omp..ompprojuser as a JOIN omp..ompuser AS b ON a.userid=b.userid "\
            "WHERE b.cadcuser IS NOT NULL "\
            "  AND (a.capacity = 'PI' OR a.capacity = 'COI') "\
            "  AND a.projectid IN "\
            "(SELECT projectid from omp..ompproj "\
            "  WHERE telescope=@t AND semester !='MEGA' AND semester !='JAC')"\
            "ORDER BY a.projectid, b.cadcuser")
        args={'@t': telescope}

        with self.db.transaction(read_write=False) as c:
            c.execute(query, {})
            rows = c.fetchall()
            results = [projectuser(*i) for i in rows]
        return results

    def get_projectids(self, semester, telescope='JCMT'):
        """
        Get all the projects from the OMP for a given semester and telescope.

        Returns a list of projectids as strings.
        """

        query = ("SELECT projectid FROM omp..ompproj WHERE semester=@s AND telescope=@t")
        args = {'@s': semester, '@t': telescope}

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            rows = [i[0] for i in rows]

        return rows

    def rename_project(self, project_old, project_new):
        """
        Change all the OMP database tables which refer to the given
        project to refer to it by the new name.
        """

        tables = [
            'ompfaultassoc',
            'ompfeedback',
            'ompmsb',
            'ompmsbdone',
            'ompobs',
            'ompobs',
            'ompproj',
            'ompprojaffiliation',
            'ompprojqueue',
            'ompprojuser',
            'ompprojuser_order',
            'ompsciprog',
            'ompsciprog_id',
            'omptimeacct',
        ]

        # First check the "new" project doesn't already exist (so that we
        # don't muddle them up).
        with self.db.transaction(read_write=False) as c:
            for table in tables:
                c.execute(
                    'SELECT COUNT(*) FROM omp..{} WHERE projectid=@n'.format(table),
                    {'@n': project_new})

                n_existing = c.fetchall()[0][0]

                if n_existing != 0:
                    raise OMPDBError(
                        'project code {} already exists in table {}'.format(
                            project_new, table))

        # Then go ahead and change the project identifier.
        with self.db.transaction(read_write=True) as c:
            for table in tables:
                c.execute(
                    'UPDATE omp..{} SET projectid=@n WHERE projectid=@o'.format(table),
                    {'@n': project_new, '@o': project_old})
