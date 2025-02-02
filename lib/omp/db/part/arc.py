# TODO: insert copyright
#
# TODO: insert license

__author__ = "Russell O. Redman"

from datetime import datetime, timedelta
import logging

from omp.db.db import OMPDB
from omp.siteconfig import get_omp_siteconfig

logger = logging.getLogger(__name__)


class ArcDB(OMPDB):
    def __init__(self):
        """
        Create a new connection to the MySQL server
        """

        config = get_omp_siteconfig()

        if config.get('hdr_database', 'driver') != 'mysql':
            raise Exception('Configured header database is not MySQL')

        OMPDB.__init__(
            self,
            server=config.get('hdr_database', 'server'),
            user=config.get('hdr_database', 'user'),
            password=config.get('hdr_database', 'password'),
            read_only=True)

        self.jcmt_db = 'jcmt.'
        self.omp_db = 'omp.'

    def read(self, query, params={}):
        """
        Run an sql query, multiple times if necessary, using read_connection.
        Only one read can be active at a time, protected by the read_mutex,
        but it can run in parallel with a write transaction.

        Arguments:
        query: a properly formated SQL select query
        params: dictionary of parameters to pass to execute
        """

        returnList = []
        logger.info(query)
        retry = True
        number = 0

        try:
            with self.db.transaction() as cursor:
                logger.debug('cursor obtained, exceuting query...')
                cursor.execute(query, params)
                logger.debug('query executed, fetching results...')
                returnList = cursor.fetchall()
                logger.debug('results fetched')

        except Exception as e:
            logger.exception('database read failed')
            raise

        return returnList

    def get_project_pi_title(self, project_id):
        """
        Retrieve the PI name and project title for the given project.

        Returns a (project_pi, project_title) tuple, of which one or both
        elements may be None if the project as a whole, or the PI name,
        could not be found.
        """

        project_pi = None
        project_title = None

        sqlcmd = '\n'.join([
            'SELECT ',
            '    ou.uname,',
            '    op.title',
            'FROM omp.ompproj op',
            '    LEFT JOIN omp.ompuser ou'
            '        ON op.pi=ou.userid AND ou.obfuscated=0',
            'WHERE op.projectid="%s"' % (project_id,)])
        answer = self.read(sqlcmd)

        if len(answer):
            project_pi = answer[0][0]
            project_title = answer[0][1]

        return (project_pi, project_title)

    # Fields to extract from various tables in the query_table method.
    query_table_columns = {
        'COMMON': (
            'atstart',
            'backend',
            'date_end',
            'date_obs',
            'elstart',
            'humstart',
            'inbeam',
            'instrume',
            'object',
            'obsdec',
            'obsdecbl',
            'obsdecbr',
            'obsdectl',
            'obsdectr',
            'obsid',
            'obsgeo_x',
            'obsgeo_y',
            'obsgeo_z',
            'obsnum',
            'obsra',
            'obsrabl',
            'obsrabr',
            'obsratl',
            'obsratr',
            'obs_type',
            'project',
            'release_date',
            'sam_mode',
            'scan_pat',
            'seeingst',
            'standard',
            'survey',
            'sw_mode',
            'tau225st',
        ),

        'ACSIS': (
            'bwmode',
            'freq_sig_lower',
            'freq_sig_upper',
            'freq_img_lower',
            'freq_img_upper',
            'ifchansp',
            'obsid_subsysnr',
            'molecule',
            'obs_sb',
            'restfreq',
            'iffreq',
            'ifchansp',
            'sb_mode',
            'ssysobs',
            'ssyssrc',
            'subsysnr',
            'transiti',
            'zsource',
        ),

        'SCUBA2': (
            'obsid_subsysnr',
            'filter',
            'wavelen',
            'bandwid',
        )}

    def query_table(self, table, obsid):
        """
        Query a specified table for a set of columns.

        Arguments:
        table      the name of the table to query
        obsid      obsid to search for

        Returns:
        A list of dictionaries keyed on the column_name.
        If a value is null, a default value will be returned in its place
        that depends upon the data_type.
        """
        columns = self.query_table_columns[table]
        selection = ',\n'.join(['    ' + key
                                for key in columns])
        sqlcmd = '\n'.join(['SELECT',
                            '%s' % (selection,),
                            'FROM ' + self.jcmt_db + table,
                            'WHERE obsid = "%s"' % (obsid,)])

        answer = self.read(sqlcmd)
        logger.debug('query complete')
        rowlist = []
        for row in answer:
            rowdict = {}
            for key, value in zip(columns, row):
                rowdict[key] = value
            rowlist.append(rowdict)
        logger.info(repr(rowlist))

        return rowlist

    def get_files(self, obsid, with_info=False):
        """
        Get the list of files in this observations, grouped obsid_subsysnr
        and sorted alphabetically.

        Arguments:
        obsid: the observation identifier for the observation
        with_info: return (name, size, md5sum) dictionaries instead
        """
        sqlcmd = '\n'.join([
            'SELECT ',
            '    obsid_subsysnr, file_id, filesize, md5sum',
            'FROM ' + self.jcmt_db + 'FILES',
            'WHERE obsid="%s"' % (obsid,),
            'ORDER BY obsid_subsysnr, file_id'])
        answer = self.read(sqlcmd)
        logger.debug('query complete')

        if not answer:
            return None

        results = {}

        for row in answer:
            (obsid_subsysnr, filename, filesize, md5sum) = row
            if obsid_subsysnr not in results:
                results[obsid_subsysnr] = []

            if with_info:
                results[obsid_subsysnr].append({
                    'name': filename,
                    'size': filesize,
                    'md5sum': md5sum,
                })
            else:
                results[obsid_subsysnr].append(filename)

        return results

    def get_heterodyne_product_info(self, backend, obsid):
        """
        Retrive the information required to derive the product ID for
        heterodyne observations.

        Returns a list of (subsysnr, restfreq, bwmode, specid, hybrid,
        ifchansp) results.

        Note: the 'hybrid' result is actual the count of subsystems: the
        observation might be a hybrid if hybrid > 1.
        """

        if backend == 'ACSIS':
            sqlcmd = '\n'.join([
                     'SELECT a.subsysnr,',
                     '       min(a.restfreq),',
                     '       min(a.bwmode),',
                     '       min(aa.subsysnr),',
                     '       count(aa.subsysnr),',
                     '       min(a.ifchansp) ',
                     'FROM ' + self.jcmt_db + 'ACSIS a',
                     '    INNER JOIN ' + self.jcmt_db + 'ACSIS aa',
                     '        ON a.obsid=aa.obsid',
                     '        AND a.restfreq=aa.restfreq',
                     '        AND a.iffreq=aa.iffreq',
                     '        AND a.ifchansp=aa.ifchansp',
                     'WHERE a.obsid = "%s"' % (obsid,),
                     'GROUP BY a.subsysnr'])

        elif backend in ['DAS', 'AOS-C']:
            sqlcmd = '\n'.join([
                     'SELECT a.subsysnr,',
                     '       a.restfreq,',
                     '       a.bwmode,',
                     '       a.specid,',
                     '       count(aa.subsysnr)',
                     'FROM ' + self.jcmt_db + 'ACSIS a',
                     '    INNER JOIN ' + self.jcmt_db + 'ACSIS aa',
                     '        ON a.obsid=aa.obsid',
                     '        AND a.specid=aa.specid',
                     'WHERE a.obsid = "%s"' % (obsid,),
                     'GROUP BY a.subsysnr, a.restfreq, a.bwmode, a.specid'])
        else:
            raise Exception('backend = ' + backend + ' is not supported')

        return self.read(sqlcmd)

    def get_obs_bounds(self,
                       project=None, date_start=None, date_end=None,
                       instrument=None, not_instrument=None, backend=None,
                       map_width=None, map_height=None,
                       rest_freq=None, if_freq=None, bw_mode=None,
                       obs_num=None, science_only=False, acsis_info=False,
                       project_info=False, map_info=False,
                       project_map_info=False, date_obs_info=False,
                       object_info=False, no_freq_sw=False,
                       exclude_known_bad=True, proprietary=None,
                       proprietary_date=None,
                       allow_ec_cal=False, obstype=None,
                       inbeam_pol=False, inbeam_fts=False, inbeam_null=False):
        """
        Fetch the bounds from the JCMT COMMON table.
        """

        conditions = []
        params = {}
        needs_acsis = False
        fields = ['obsratl', 'obsrabl', 'obsratr', 'obsrabr',
                  'obsdectl', 'obsdecbl', 'obsdectr', 'obsdecbr']

        if project is not None:
            conditions.append('project=%(p)s')
            params['p'] = project
        else:
            if not allow_ec_cal:
                conditions.append('project NOT LIKE "%%EC%%"')
                conditions.append('project <> "JCMTCAL"')
                conditions.append('project <> "CAL"')

        if date_start is not None:
            conditions.append('utdate>=%(ds)s')
            params['ds'] = int(date_start)

        if date_end is not None:
            conditions.append('utdate<=%(de)s')
            params['de'] = int(date_end)

        if instrument is not None:
            conditions.append('UPPER(instrume)=%(i)s')
            params['i'] = instrument.upper()
        elif not_instrument is not None:
            conditions.append('UPPER(instrume)<>%(ni)s')
            params['ni'] = not_instrument.upper()

        if inbeam_pol:
            conditions.append('inbeam LIKE "%%pol%%"')
        if inbeam_fts:
            conditions.append('inbeam LIKE "%%fts%%"')
        if inbeam_null:
            conditions.append('inbeam IS NULL')

        if backend is not None:
            conditions.append('UPPER(backend)=%(be)s')
            params['be'] = backend.upper()

        if map_width is not None:
            conditions.append('map_wdth=%(w)s')
            params['w'] = map_width

        if map_height is not None:
            conditions.append('map_hght=%(h)s')
            params['h'] = map_height

        if obs_num is not None:
            conditions.append('obsnum=%(obs)s')
            params['obs'] = obs_num

        if acsis_info:
            needs_acsis = True
            # Give sam_mode first so that it can be found for
            # turning raster into scan...
            fields.extend(('sam_mode', 'sw_mode', 'obs_type',
                           'restfreq',
                           'iffreq',
                           'bwmode',
                           ))

        if rest_freq is not None:
            needs_acsis = True
            conditions.append('abs(restfreq - %(rf)s) < 0.0001')
            params['rf'] = rest_freq

        if if_freq is not None:
            needs_acsis = True
            conditions.append('abs(iffreq - %(if)s) < 0.0001')
            params['if'] = if_freq

        if bw_mode is not None:
            needs_acsis = True
            conditions.append('bwmode=%(bwm)s')
            params['bwm'] = bw_mode

        if project_info or project_map_info:
            fields.append('project')

        if map_info or project_map_info:
            fields.append('SQRT(map_wdth * map_hght) AS map_size')

        if date_obs_info:
            fields.extend(('utdate', 'obsnum'))

        if object_info:
            fields.append('object')

        if science_only:
            conditions.append('obs_type="science"')
        elif obstype is not None:
            conditions.append('obs_type=%(ot)s')
            params['ot'] = obstype

        if no_freq_sw:
            conditions.append('sw_mode<>"freqsw"')

        if exclude_known_bad:
            if instrument is None or instrument == 'SCUBA-2':
                conditions.append('(NOT (utdate=20091026 AND obsnum=36))')

            # TODO: presumably both should apply when instrument is None.
            # (But with instrument constraints since obsnum values repeat.)
            elif instrument is None or instrument == 'HARP':
                conditions.append('(NOT (utdate=20090404 AND obsnum=94))')

            elif instrument == 'RxA3':
                conditions.append('(NOT (utdate=20150814 AND obsnum=50))')

            elif instrument == 'RxA3m':
                pass

            elif instrument == 'Uu':
                pass

            elif instrument == 'Aweoweo':
                pass

            elif instrument == 'Alaihi':
                pass

            elif instrument == 'Kuntur':
                pass

            else:
                raise Exception('Unknown instrument {0}'.format(instrument))

        if proprietary is None:
            pass
        else:
            if proprietary_date is None:
                prop_date_str = 'now()'
            else:
                prop_date_str = '"' + proprietary_date + '"'
            if proprietary:
                conditions.append('release_date > ' + prop_date_str)
            else:
                conditions.append('release_date <= ' + prop_date_str)

        if conditions:
            condition = ' WHERE ' + ' AND '.join(conditions)
        else:
            condition = ''

        if needs_acsis:
            extra_table = ' LEFT JOIN jcmt.ACSIS ON ' \
                'jcmt.COMMON.obsid=jcmt.ACSIS.obsid'
        else:
            extra_table = ''

        with self.db.transaction() as c:
            c.execute('SELECT ' + ', '.join(fields) +
                      ' FROM jcmt.COMMON' + extra_table + condition,
                      params)

            return c.fetchall()

    def get_obsid_and_project(self, utdate, obsnum):
        with self.db.transaction() as c:
            c.execute(
                'SELECT obsid, project FROM jcmt.COMMON WHERE '
                'utdate=%(utdate)s AND '
                'instrume="SCUBA-2" AND '
                'obsnum=%(obsnum)s',
                {
                    'utdate': utdate.strftime('%Y%m%d'),
                    'obsnum': obsnum,
                })

            row = c.fetchall()
            obsid = row[0][0]
            project = row[0][1]

        return (obsid, project)

    def get_dates_for_project(self, project):
        result = []

        with self.db.transaction() as c:
            c.execute(
                'SELECT DISTINCT utdate from jcmt.COMMON'
                ' WHERE project = %(project)s',
                {'project': project})

            while True:
                row = c.fetchone()
                if row is None:
                    break
                result.append(row[0])

        return result

    def find_calibrator_obsnum(
            self, utdate, instrument, objects=None, obstype=None):
        args = {
            'utdate': utdate,
            'instrument': instrument,
        }
        query = (
            'SELECT obsnum '
            'FROM jcmt.COMMON WHERE utdate=%(utdate)s '
            'AND instrume=%(instrument)s '
            'AND project="JCMTCAL" '
        )

        if objects is not None:
            params = []
            for (i, object_) in enumerate(objects):
                param = 'object{}'.format(i)
                args[param] = object_
                params.append(param)
            query = query + 'AND object IN (' + ', '.join(
                ('%({})s'.format(x) for x in params)) + ') '

        if obstype is not None:
            query = query + 'AND obs_type=%(obstype)s '
            args['obstype'] = obstype

        print("Query: " + query)
        print("Params: " + repr(args))

        result = []

        with self.db.transaction() as c:
            c.execute(query, args)

            while True:
                row = c.fetchone()
                if row is None:
                    break

                result.append(row[0])

        return result

    def read_cso_opacity_data(
            self, utdate):
        utdate = datetime.strptime(utdate, '%Y%m%d')

        result = []

        with self.db.transaction() as c:
            c.execute(
                'SELECT cso_ut, tau, tau_rms '
                'FROM jcmt_tms.CSOTAU WHERE cso_ut >= %(ds)s AND cso_ut < %(de)s',
                {
                    'ds': utdate,
                    'de': (utdate + timedelta(days=1)),
                })

            while True:
                row = c.fetchone()
                if row is None:
                    break

                result.append(row)

        return result

    def read_cso_opacity_data_range(self, utc0, utc1):
        with self.db.transaction() as c:
            c.execute(
                'select cso_ut, tau, tau_rms from jcmt_tms.CSOTAU '
                'where cso_ut>=%(ds)s and cso_ut<=%(de)s ',
                {'ds': utc0, 'de': utc1})

            rows = c.fetchall()

        return rows
