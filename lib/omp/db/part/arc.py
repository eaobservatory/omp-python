# TODO: insert copyright
#
# TODO: insert license

__author__ = "Russell O. Redman"

import logging

from omp.db.db import OMPDB
from omp.siteconfig import get_omp_siteconfig

logger = logging.getLogger(__name__)


class ArcDB(OMPDB):
    def __init__(self):
        """
        Create a new connection to the Sybase server
        """

        config = get_omp_siteconfig()

        OMPDB.__init__(
            self,
            server=config.get('hdr_database', 'server'),
            user=config.get('hdr_database', 'user'),
            password=config.get('hdr_database', 'password'),
            read_only=True)

        self.jcmt_db = 'jcmt..'
        self.omp_db = 'omp..'

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

    def close(self):
        """
        Close the database conenction

        Currently not implemented.
        """

        pass

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
            'FROM omp..ompproj op',
            '    LEFT JOIN omp..ompuser ou'
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

    def get_files(self, obsid):
        """
        Get the list of files in this observations, grouped obsid_subsysnr
        and sorted alphabetically.

        Arguments:
        obsid: the observation identifier for the observation
        """
        sqlcmd = '\n'.join([
            'SELECT ',
            '    obsid_subsysnr,',
            '    file_id',
            'FROM ' + self.jcmt_db + 'FILES',
            'WHERE obsid="%s"' % (obsid,),
            'ORDER BY obsid_subsysnr, file_id'])
        answer = self.read(sqlcmd)
        logger.debug('query complete')

        results = {}
        if len(answer):
            for i in range(len(answer)):
                obsid_subsysnr = answer[i][0]
                if obsid_subsysnr not in results:
                    results[obsid_subsysnr] = []
                results[obsid_subsysnr].append(answer[i][1])
        else:
            return None

        return results

    def get_heterodyne_product_info(self, backend, obsid):
        """
        Retrive the information required to derive the product ID for
        heterodyne observations.

        Returns a list of (subsysnr, restfreq, bwmode, specid, hybrid) results.
        """

        if backend == 'ACSIS':
            sqlcmd = '\n'.join([
                     'SELECT a.subsysnr,',
                     '       min(a.restfreq),',
                     '       min(a.bwmode),',
                     '       min(aa.subsysnr),',
                     '       count(aa.subsysnr)',
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
