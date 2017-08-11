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

import logging
logger = logging.getLogger(__name__)


class OMPDB:
    """OMP and JCMT database access class.
    """

    CommonInfo = None
    FullObservationInfo = None

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

    def get_obsid_status(self, obsid, comment=False):
        """Retrieve the last comment status for a given obsid.

        If comment = True: also retrieve the last obsid comment.

        Returns None if no status was found.
        """
        query = ('SELECT commentstatus FROM omp..ompobslog '
                'WHERE obslogid = '
                '(SELECT MAX(obslogid) FROM omp..ompobslog '
                'WHERE obsid=@o AND obsactive=1)')
        args = {'@o': obsid}
        if comment:
            query = query.replace('commentstatus', 'commentstatus, commenttext, commentauthor, commentdate')

        with self.db.transaction() as c:
            c.execute(query, args)
            rows = c.fetchall()

        if not rows:
            return None

        if len(rows) > 1:
            raise OMPDBError('multiple status results for one obsid')

        if not comment:
            return rows[0][0]
        else:
            return rows[0]

    def find_obs_for_ingestion(self, utdate_start, utdate_end=None,
                               no_status_check=False, no_transfer_check=False,
                               ignore_instruments=None):
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

        # Apply instrument constraint.
        if ignore_instruments:
            where.append('(instrume not in ({}))'.format(', '.join(
                ['"{}"'.format(x) for x in ignore_instruments])))

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

    def get_observations_from_project(self, projectcode,
                                      utdatestart=None, utdateend=None, instrument=None,
                                      ompstatus=None):
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

        query = ("SELECT c.obsid, "
                 "  CASE WHEN c.recipe='REDUCE_POL_SCAN' THEN 'POL-2' ELSE c.instrume END AS instrume, "
                 " c.wvmtaust, c.wvmtauen, c.utdate, c.obsnum, c.object,"
                 " datediff(second, c.date_obs, c.date_end) as time, o.commentstatus, o.commenttext,"
                 " c.req_mintau, c.req_maxtau "
                 " FROM jcmt..COMMON as c, omp..ompobslog as o"
                 " WHERE project=@p AND c.obsid*=o.obsid "
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

        if ompstatus:
            query += ' AND o.commentstatus = @c'
            args['@c'] = ompstatus
        # Order by date.
        query += ' ORDER BY c.utdate ASC '

        projobsinfo = namedtuple('projobsinfo',
            'obsid instrument wvmtaust wvmtauen utdate obsnum object duration '
            'status commenttext req_mintau req_maxtau')

        # Carry out query
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = OrderedDict( [ [i[0], projobsinfo(*i)] for i in rows] )
        return results

    def create_group_project_query(self, semester=None, queue=None, projects=None, patternmatch=None, telescope='JCMT'):
        """
        Create a WHERE selection and a projectID selection from omp..ompproj ASp and omp..ompprojqueue AS q

        Returns a WHERE query list (to be joined with " AND ".join(wherequery), a where args dictionary,
        and a 'FROM' statement, and a select statement

        Args used are: @queue, @semester, @pattern and @ telescope.

        Not 100% safe for projects, as it formats them directly.
        """
        wherequery = []
        args = {}
        fromstatement = "FROM omp..ompproj AS p "
        selectstatement = "SELECT p.projectid "
        if queue:
            wherequery += [" q.country=@queue "]
            args['@queue'] = queue
            fromstatement += "JOIN omp..ompprojqueue AS q  ON p.projectid=q.projectid "

        if semester:
            wherequery += [" p.semester=@semester "]
            args['@semester'] = semester

        if projects is not None and projects != []:
            projstring = ", ".join(["'{}'".format(i) for i in projects])
            wherequery += [" p.projectid in ({}) ".format(projstring)]

        if patternmatch:
            wherequery += [" p.projectid LIKE @pattern "]
            args['@pattern'] = patternmatch

        if telescope:
            wherequery += [" p.telescope=@telescope "]
            args['@telescope'] = telescope

        return selectstatement, fromstatement, wherequery, args

    def get_summary_obs_info_group(self, semester=None, queue=None, projects=None,
                                   patternmatch=None,  utdatestart=None, utdateend=None,
                                   csotau=False):

        """
        Return summary information about observations for a group of projects.
        """

        projobsinfo = namedtuple('projobsinfo', 'project instrument band status number totaltime daynight')
        # First select groups of projects

        if semester is not None or queue is not None  or projects is not None or patternmatch is not None:
            selectstatement, fromstatement, wherelist, args = self.create_group_project_query(
                semester=semester, queue=queue, projects=projects, patternmatch=patternmatch,
                telescope='JCMT')
            where = ' WHERE ' + ' AND '.join(wherelist)
            projectselect = " AND project in ({} {} {}) ".format(selectstatement, fromstatement, where)
            projectselect = "{} {} {} ".format(selectstatement, fromstatement, where)
            with self.db.transaction(read_write=False) as c:
                c.execute(projectselect, args)
                projects = c.fetchall()
            projects = [i[0] for i in projects]
            projectselect = " AND project in (" + ', '.join(["'" + p + "'" for p in projects]) +") "
            args = {}
        else:
            projectselect = ""
            args = {}
        datequery = ''
        if utdatestart:
            datequery += ' AND utdate >= @datestart '
            args['@datestart'] = utdatestart
        if utdateend:
            datequery += ' AND utdate <= @dateend '
            args['@dateeend'] = utdateend


        select_inner = ("SELECT c.project, "
                 "             CASE WHEN c.recipe='REDUCE_POL_SCAN' THEN 'POL-2' ELSE c.instrume "
                 "             END AS instrume, "
                 "             datediff(second, c.date_obs, c.date_end) as duration, "
                 "             CASE WHEN o.commentstatus is NULL "
                 "                  THEN 0 "
                 "                  ELSE o.commentstatus "
                 "             END AS commentstatus, "
                 "             CASE WHEN (wvmtaust+wvmtauen)/2.0 between 0.005    and 0.05 then '1' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.05 and 0.08 then '2' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.08 and 0.12 then '3' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.12 and 0.2  then '4' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.2  and 100  then '5' "
                 "                  ELSE 'unknown' "
                 "             END AS band, "
                 "             CASE WHEN datepart(hh, date_obs)+datepart(mi, date_obs)/60.0 "
                 "                       between 3.5 and 19.5 THEN 'night' "
                 "                  ELSE 'day' "
                 "             END AS daynight "
                 "      FROM jcmt..COMMON AS c, omp..ompobslog AS o "
             )

        if csotau:
            select_inner = ("SELECT c.project, c.instrume, datediff(second, c.date_obs, c.date_end) as duration, " \
                 "             CASE WHEN o.commentstatus is NULL "\
                 "                  THEN 0 "\
                 "                  ELSE o.commentstatus "\
                 "             END AS commentstatus, "\
                 "             CASE WHEN (tau225st+tau225en)/2.0 between 0.005 and 0.05 then '1' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.05 and 0.08 then '2' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.08 and 0.12 then '3' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.12 and 0.2  then '4' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.2  and 100  then '5' "\
                 "                  ELSE 'unknown' "\
                 "             END AS band, "\
                 "             CASE WHEN datepart(hh, date_obs)+datepart(mi, date_obs)/60.0"
                 "                       between 3.5 and 19.5 THEN 'night'"\
                 "                  ELSE 'day' "\
                 "             END AS daynight "
                 "      FROM jcmt..COMMON AS c, omp..ompobslog AS o "\
             )

        where_inner = ("      WHERE c.obsid*=o.obsid "+ projectselect +
                       "       AND o.obslogid IN (SELECT MAX(obslogid) FROM omp..ompobslog GROUP by obsid) "
                       + datequery
                   )

        select_outer = ("SELECT t.project, t.instrume, t.band, t.commentstatus, " \
                 "       count(*) as numobs, sum(t.duration) as totaltime, t.daynight " \
              )

        from_outer = " FROM ( " + select_inner + where_inner + " ) t "
        group_outer = (" GROUP BY t.project, t.instrume, t.band, t.commentstatus, t.daynight "\
                 "ORDER BY t.project, t.instrume, t.band ASC, t.commentstatus ASC ")
        query = select_outer + from_outer + group_outer
        print(select_outer)
        print(from_outer)
        print(group_outer)
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [projobsinfo(*i) for i in rows]

        return results

    def get_summary_obs_info(self, projectpattern, like=True, utdatestart=None, utdateend=None, csotau=False):

        """Get summary of obs info for projects.

        Gets the number and duration of observations per project, split

        up by omp status, weatherband and instrument.

        projectpattern (str): value used in query against COMMON.project
        like: if to use 'like' project matching or '='
        utdatestart (int): inclusive start UT date
        utdateend (int): inclusive end UT date

        csotau (bool): if True, use tau225 header instead of wvmtau header.
        daytime (str): can be None, 'day', 'night'. nighttime
           is for obs from 03:30AM to 19:30 UT (5:30PM to 9:30AM HST)

        """
        projobsinfo = namedtuple('projobsinfo', 'project instrument band status number totaltime daynight')

        args = {}
        if projectpattern:
            args['@p'] =  projectpattern
        datequery = ''
        if utdatestart:
            datequery += ' AND utdate >= @s '
            args['@s'] = utdatestart
        if utdateend:
            datequery += ' AND utdate <= @e '
            args['@e'] = utdateend



        select_inner = ("SELECT c.project, c.instrume, datediff(second, c.date_obs, c.date_end) as duration, "
                 "             CASE WHEN o.commentstatus is NULL "
                 "                  THEN 0 "
                 "                  ELSE o.commentstatus "
                 "             END AS commentstatus, "
                 "             CASE WHEN (wvmtaust+wvmtauen)/2.0 between 0.005    and 0.05 then '1' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.05 and 0.08 then '2' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.08 and 0.12 then '3' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.12 and 0.2  then '4' "
                 "                  WHEN (wvmtaust+wvmtauen)/2.0 between 0.2  and 100  then '5' "
                 "                  ELSE 'unknown' "
                 "             END AS band, "
                 "             CASE WHEN datepart(hh, date_obs)+datepart(mi, date_obs)/60.0 "
                 "                       between 3.5 and 19.5 THEN 'night' "
                 "                  ELSE 'day' "
                 "             END AS daynight "
                 "      FROM jcmt..COMMON AS c, omp..ompobslog AS o "
             )

        if csotau:
            select_inner = ("SELECT c.project, c.instrume, datediff(second, c.date_obs, c.date_end) as duration, " \
                 "             CASE WHEN o.commentstatus is NULL "\
                 "                  THEN 0 "\
                 "                  ELSE o.commentstatus "\
                 "             END AS commentstatus, "\
                 "             CASE WHEN (tau225st+tau225en)/2.0 between 0.005 and 0.05 then '1' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.05 and 0.08 then '2' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.08 and 0.12 then '3' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.12 and 0.2  then '4' "\
                 "                  WHEN (tau225st+tau225en)/2.0 between 0.2  and 100  then '5' "\
                 "                  ELSE 'unknown' "\
                 "             END AS band, "\
                 "             CASE WHEN datepart(hh, date_obs)+datepart(mi, date_obs)/60.0"
                 "                       between 3.5 and 19.5 THEN 'night'"\
                 "                  ELSE 'day' "\
                 "             END AS daynight "
                 "      FROM jcmt..COMMON AS c, omp..ompobslog AS o "\
             )

        if projectpattern:
            projectcomparison = ' AND project LIKE @p '
            if not like:
                projectcomparison = ' AND project=@p '
        else:
            projectcomparison = ""

        where_inner = ("      WHERE c.obsid*=o.obsid "+ projectcomparison +
                       "            AND o.obslogid IN (SELECT MAX(obslogid) FROM omp..ompobslog GROUP by obsid) "
                       + datequery
                   )

        select_outer = ("SELECT t.project, t.instrume, t.band, t.commentstatus, " \
                 "       count(*) as numobs, sum(t.duration) as totaltime, t.daynight " \
              )
        from_outer = " FROM ( " + select_inner + where_inner + " ) t "
        group_outer = (" GROUP BY t.project, t.instrume, t.band, t.commentstatus, t.daynight "\
                 "ORDER BY t.project, t.instrume, t.band ASC, t.commentstatus ASC ")
        query = select_outer + from_outer + group_outer

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [projobsinfo(*i) for i in rows]

        return results

    def get_summary_msb_info_group(self, semester=None, queue=None, projects=None, patternmatch=None):
        """Get overview of the msbs waiting to be observed for a group of projects.

        Projects determined by semester, queue, list of project ids,
        and/or a patternmatch (including the wildcards,
        e.g. patternmatch='%EC%').  All constraints are combined with
        an AND.

        Returns a list of namedtuples, each namedtuple represents the
        summary for one project that matches the constraints.

        """
        if semester or queue:
            selectstatement, fromstatement, wherelist, args = self.create_group_project_query(
                semester=semester, queue=queue, projects=projects, patternmatch=patternmatch,
                telescope='JCMT')
            where = ' WHERE ' + ' AND '.join(wherelist)
            projectselect = " o.projectid in ({} {} {}) ".format(selectstatement, fromstatement, where)

        else:
            projectselect = []
            args = {}
            if projects:
                projectselect += ["o.projectid in (" + ','.join(["'" + p + "'" for p in projects]) + ')']
            if patternmatch:
                projectselect += ["o.projectid like @pattern"]
                args['@pattern'] = patternmatch
            projectselect = ' AND ' .join(projectselect)
        projmsbinfo = namedtuple('projmsbinfo', 'project uniqmsbs totalmsbs totaltime taumin taumax')
        query = ("SELECT o.projectid, count(*), sum(o.remaining), "\
                 "       sum(o.timeest*o.remaining), o.taumin, o.taumax "\
                 "FROM omp..ompmsb as o ")
        where = " WHERE o.remaining > 0 "
        where += " AND " + projectselect
        group = " GROUP BY o.taumin, o.taumax, o.projectid ORDER BY o.projectid, o.taumin, o.taumax"

        query += where
        query += group

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [projmsbinfo(*i) for i in rows]

        return results

    def get_summary_msb_info(self, projectpattern):
        """Get overview of the msbs waiting to be observed.

        Returns a list of namedtuples, each namedtuple represents the
        summary for one tau range for one project that matches the projectpattern.

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

    def get_time_charged_group(self, semester=None, queue=None, projects=None,
                               patternmatch=None, telescope='JCMT'):
        """
        Return time charged per day by project.

        Project constraints are combined with an AND.

        Returns Dictionary, key being a project code, value being a
        list of namedtuples, orderd by Dated.

        """
        query = ("SELECT t.projectid, t.date, t.timespent, t.confirmed FROM omp..omptimeacct AS  t "
                 " JOIN omp..ompproj AS p ON t.projectid=p.projectid "
                 " JOIN omp..ompprojqueue AS q ON t.projectid=q.projectid ")
        args = {}
        wherequery = []

        if queue:
            wherequery += [" q.country=@q "]
            args['@q'] = queue

        if semester:
            wherequery += [" p.semester=@sem "]
            args['@sem'] = semester
        if projects:
            projstring = ", ".join(["'{}'".format(i) for i in projects])
            wherequery += [" p.projectid in ({}) ".format(projstring)]

        if patternmatch:
            wherequery += [" p.projectid LIKE @pattern "]
            args['@pattern'] = patternmatch

        if telescope:
            wherequery += [" p.telescope=@telescope "]
            args['@telescope'] = telescope

        query += " WHERE " + " AND ".join(wherequery)

        query += " ORDER BY t.date ASC "

        timeinfo = namedtuple('timeinfo', 'date timespent confirmed')

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()

        # Sort out the rows
        projects = set([i[0] for i in rows])
        results = {}
        for r in rows:
            p = r[0]
            vals = r[1:]
            info  = timeinfo( *vals )
            results[p] = results.get(p, []) + [info]

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


    def get_fault_summary_group(self, semester=None, queue=None, projects=None, patternmatch=None):
        """
        Get summary of faults for a group of projects.

        All project constraints are combined with an AND.

        Returns list of faultinfo object, one per project found.
        """
        if semester or queue:
            selectstatement, fromstatement, wherelist, args = self.create_group_project_query(
                semester=semester, queue=queue, projects=projects, patternmatch=patternmatch,
                telescope='JCMT')
            where = ' WHERE ' + ' AND '.join(wherelist)
            projectselect = " a.projectid in ({} {} {}) ".format(selectstatement, fromstatement, where)

        else:
            projectselect = []
            args = {}
            if projects:
                projectselect += ["a.projectid in (" + ','.join(["'" + p + "'" for p in projects]) + ')']
            if patternmatch:
                projectselect += ["a.projectid like @pattern"]
                args['@pattern'] = patternmatch
            projectselect = ' AND ' .join(projectselect)

        query = ("SELECT a.projectid, f.faultid, f.status, f.subject "\
                 "FROM omp..ompfaultassoc as a JOIN omp..ompfault as f "\
                 "ON a.faultid = f.faultid ")
        query += " WHERE {}".format(projectselect)

        faultinfo = namedtuple('faultinfo', 'project faultid status subject')
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [faultinfo(*i) for i in rows]
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

    def get_allocations(self, semester=None, queue=None, projects=None, patternmatch=None, telescope='JCMT'):
        """
        Return allocation information for a group of projects.

        """
        allocinfo = namedtuple('allocinfo',
                               'pi title semester allocated remaining pending taumin taumax priority enabled')

        selectstatement, fromstatement, wherequery, args = self.create_group_project_query(
            semester=semester, queue=queue, projects=projects, patternmatch=patternmatch,
            telescope=telescope)

        query = ("SELECT p.projectid, p.pi, p.title, p.semester, p.allocated, p.remaining, "
                 "p.pending, p.taumin, p.taumax, q.tagpriority, p.state FROM omp..ompproj AS p "
                 " JOIN omp..ompprojqueue  AS q ON p.projectid=q.projectid ")

        query += " WHERE " + " AND ".join(wherequery)
        query += " ORDER BY q.tagpriority"

        with  self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()

            results = OrderedDict([[i[0], allocinfo(*i[1:])] for i in rows])
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

        query = ("SELECT projectid, pi, title, semester, allocated, remaining, pending, taumin, taumax"
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

        Excludes semester='MEGA' and semesters='JAC' or 'EAO'.

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
            "  WHERE telescope=@t AND semester !='MEGA' AND semester !='JAC' AND semester != 'EAO')"\
            "ORDER BY a.projectid, b.cadcuser")
        args={'@t': telescope}

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
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


    def get_support_projects(self, userid, semester):
        """
        Return all projects supported by a given userid for a given semester.

        """
        query = ("SELECT p.projectid FROM omp..ompproj AS p JOIN omp..ompprojuser AS u ON p.projectid=u.projectid " \
                " WHERE u.userid=@u AND u.capacity='SUPPORT' AND p.semester=@s")
        args = {'@u': userid, '@s': semester}
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            projects = c.fetchall()
        projects = [i[0] for i in projects]
        return projects

    def get_jcmt_observed_projects(self, utdatestart, utdateend):
        """
        Get all projects observed between two dates.
        utdatestart (int): inclusive start UT date
        utdateend (int): inclusive end UT date

        Return a list of namedtuples with project, semester and country and tagpriority
        """
        projinfo = namedtuple('projinfo', 'project semester country tagpriority')

        query = ("SELECT p.projectid, p.semester, q.country, q.tagpriority "
                 "FROM omp..ompproj AS p JOIN omp..ompprojqueue AS q ON p.projectid=q.projectid "
                 "WHERE p.projectid IN ( "
                 "   SELECT DISTINCT project FROM jcmt..COMMON WHERE utdate>=@s AND utdate<=@e "
                 " ) "
                 "ORDER BY p.semester, q.country, p.projectid"
                 )
        args = {'@s': utdatestart, '@e': utdateend}

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            rows = c.fetchall()
            results = [projinfo(*i) for i in rows]

        return results

    def get_acsis_info(self, projectcode):
        """
        """
        query = ("SELECT obsid, molecule, transiti, bwmode, subsysnr, doppler, zsource, restfreq "
                 " FROM jcmt..ACSIS WHERE obsid in (SELECT obsid from jcmt..COMMON where project=@p)")
        args = {'@p': projectcode}
        acsisInfo = namedtuple('acsisInfo', "obsid, molecule transition bwmode subsysnr doppler zsource restfreq")
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            values = c.fetchall()
        if not values:
            return None
        else:
            return [acsisInfo(*i) for i in values]

    def get_observations(self, projectcode, utdatestart=None, utdateend=None, ompstatus=None):
        """Get a project's observations, optionally limited by date/status.

        Returns a NamedTuple object containing everything from the
        JCMT COMMON Table, as well as the most recent comment
        information from the omp ompobslog table. If there is no
        comment in the ompobslog, it will assume a status of 0 (good)
        and return 'None' for the commenttext, commentauthor and
        commentdate.

        Note: This uses the 'utdate' column in the COMMON
        table. Ocassionally (usually for observations that extend over
        the utdate chang) this value may not be what you expect.

        Arguments:
           projectcode, str: project ID (as it appears in COMMON, usually uppercase)

        Keywords:
           utdatestart, int: YYYYMMDD Only include obs with obsid on or after this date
           utdateend, int: YYYYMMDD Only include obs taken on or before this date.

        """
        query = ("SELECT c.*, "
                 " CASE WHEN p.commentstatus is NULL THEN 0 ELSE p.commentstatus END AS commentstatus, "
                 " p.commenttext, p.commentauthor, p.commentdate "
                 " FROM omp..ompobslog AS p, jcmt..COMMON AS c "
                 " WHERE c.obsid*=p.obsid AND obslogid IN "
                 "   (SELECT MAX(obslogid) from omp..ompobslog WHERE obsactive=1 GROUP BY obsid) "
                 "  AND c.project=@p ")

        args = {'@p': projectcode}

        if utdatestart:
            query += ' AND c.utdate >= @s '
            args['@s'] = utdatestart
        if utdateend:
            query += ' AND c.utdate <= @e '
            args['@e'] = utdateend

        # If wanting to exclude NULL values, change query.
        if ompstatus and ompstatus != 0:
            query = query.replace('*=', '=')
            query += ' AND p.commentstatus = @c'
            args['@c'] = ompstatus

        elif ompstatus and ompstatus == 0:
            query += ' AND p.commentstatus = @c'
            args['@c'] = ompstatus

        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            values = c.fetchall()
            cols = c.description

        if not values:
            return None

        if self.FullObservationInfo is None:
            self.FullObservationInfo = namedtuple(
                'FullObservationInfo',
                ['{0}_'.format(x[0]) if iskeyword(x[0]) else x[0]
                 for x in cols])

        return [self.FullObservationInfo(*i) for i in values]

    def get_remaining_msb_info(self, projectcode):
        """Return msb information for project.

        Returns a tuple of list of results and column names.

        If there are no results, returns a tuple with an empty list
        for the results.

        """
        query = ("SELECT pol, instrument, title, wavelength, target, coordstype, ra2000, dec2000, remaining, "
                 " a.timeest,  taumin, taumax, priority "
                 " FROM omp..ompobs AS a JOIN omp..ompmsb  as m ON a.msbid=m.msbid "
                 " WHERE m.projectid=@p AND m.remaining > 0 ")
        args = {'@p': projectcode}
        with self.db.transaction(read_write=False) as c:
            c.execute(query, args)
            results = c.fetchall()
            cols = c.description

        MsbInfo= namedtuple(
                'MsbInfo',
                ['{0}_'.format(x[0]) if iskeyword(x[0]) else x[0]
                 for x in cols])
        return [MsbInfo(*i) for i in results]

    def get_project_info(self, projectcode):
        """
        Return project title, semester, hours_assigned, hours_used, taumin, tamx,

        Also:
        PI

        """
        projinfo = namedtuple('projinfo', 'id title semester allocated_hours remaining_hours opacityrange state pi fops cois')
        userinfo = namedtuple('userinfo', 'userid uname email cadcuser contactable')

        query_users = ("SELECT u.userid, uname, email, cadcuser, contactable, capacity "
                       "FROM omp..ompprojuser AS pu  JOIN omp..ompuser AS u ON pu.userid=u.userid "
                       "WHERE projectid=@p")

        query_proj = ("SELECT projectid, title, semester, allocated/(60.0*60.0), remaining/(60.0*60.0), taumin, taumax, state "
                 "FROM omp..ompproj "
                 "WHERE projectid=@p")

        args = {'@p': projectcode}

        with self.db.transaction(read_write=False) as c:
            c.execute(query_users, args)
            uservalues = c.fetchall()
            fops = []
            pi = []
            cois = []
            for i in uservalues:
                if i[-1] == 'PI':
                    pi.append(userinfo(*i[0:-1]))
                elif i[-1] == 'COI':
                    cois.append(userinfo(*i[0:-1]))
                elif i[-1] == 'SUPPORT':
                    fops.append(userinfo(*i[0:-1]))
                else:
                    logger.warning('User {} in project {} has an unknown capacity {}'.format(i[0], projectcode, i[4]))
            c.execute(query_proj, args)
            projvalues = c.fetchall()
        if len(projvalues) > 1:
            raise OMPDBError('Multiple projects found for 1 ID!')
        if len(projvalues) == 0:
            raise OMPDBError('No project found for {}'.format(projectcode))
        projvalues = list(projvalues[0])
        projvalues = projvalues[0:5] + [(projvalues[5], projvalues[6])] + projvalues[7:] + [pi] + [fops] + [cois]
        project_info = projinfo(*projvalues)
        return project_info


    def get_project_time_by_weatherband(queue=None, project=None, utdatestart=None, utdateend=None):
        """Return minutes spend (by observation duration) per project


        Either queue or project must be given. Project will be
        evaluated on a 'LIKE' basis.

        Please note: this does not exclude JUNK/BAD/QUESTIONABLE
        observations from the time calculation!

        Time charged to projects is different from this: it will also
        include a share of time gaps from each night, exclude JUNK/BAD
        observations, and may include other charging variations due to
        e.g. charging half time for observing in a worse weather band,
        human error etc.

        """

