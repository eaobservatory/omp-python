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
