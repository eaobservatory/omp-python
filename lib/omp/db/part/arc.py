__author__ = "Russell O. Redman"


import errno
from contextlib import contextmanager
import datetime
import logging
import os.path
import re
import subprocess
from threading import Event
from threading import Lock
import traceback

try:
    import Sybase
    sybase_defined = True
except ImportError:
    sybase_defined = False

from tools4caom2 import __version__
from tools4caom2.error import CAOMError

__doc__ = """
The database class immplements thread-safe methods methods to interact with
Sybase databases.

Version: """ + __version__.version

logger = logging.getLogger(__name__)


class database(object):
    """
    Manage connection to Sybase.
    Credentials are read from userconfig, which is a SafeConfigParser object.

    The userconfig dictionary can contain definitions for:
    'server': Sybase server

    'cred_id': database user account
    'cred_key': database user password

    'read_db': database to read by default ('cred_db' if absent)

    It is legitimate to omit all of these if no connection is needed.

    Usage:
    userconfig['server'] = 'SYBASE'

    with database(userconfig) as db:
        cmd = 'SELECT max(utdate) from ' + jcmt_db + 'COMMON'
        max_utdate = db.read(cmd)[0][0]

    This class creates separate singleton connections for read and write
    operations, protected with a mutex so the code should be thread-safe.
    """

    # class attribute read_mutex
    read_mutex = Lock()

    # class attribute read_connection
    read_connection = None

    def __init__(self, userconfig, use=True):
        """
        Create a new connection to the Sybase server

        Arguments:
        userconfig: SafeConfigParser object

        Exceptions:
        IOError: if call to dbrc_get fails
        IOError: if dbrc_get cannot read credentials for SYBASE jcmtmd.dbo
        IOError: if connection to SYBASE fails

        It is legitimate to customize the pause_queue for each connection.
        """
        if sybase_defined:
            self.use = use
        else:
            self.use = False

        # Database server to use for queries
        self.server = None

        # Database account and password
        self.cred_id = None
        self.cred_key = None

        # Database to use for default read and write access
        self.read_db = None

        # Always read the userconfig to configure database connection
        if userconfig.has_section('database'):
            if userconfig.has_option('database', 'server'):
                self.server = userconfig.get('database', 'server')

            if userconfig.has_option('database', 'read_db'):
                self.read_db = userconfig.get('database', 'read_db')

            if userconfig.has_option('database', 'cred_id'):
                self.cred_id = userconfig.get('database', 'cred_id')
            if userconfig.has_option('database', 'cred_key'):
                self.cred_key = userconfig.get('database', 'cred_key')
        else:
            self.use = False

        logger.debug('sybase_defined: %s', sybase_defined)
        logger.debug('database use: %s', self.use)
        if self.server:
            logger.debug('database server: ' + self.server)
        if self.read_db:
            logger.debug('database read_db: ' + self.read_db)
        self.pause_queue = [1.0, 2.0, 3.0]
        # if self.cred_id:
        #     self.log.file('database cred_id: ' + self.cred_id, logging.DEBUG)
        # if self.cred_key:
        #     self.log.file('database cred_key: ' + self.cred_key,
        #                   logging.DEBUG)

    def available(self):
        """
        Return True if it is possible to query the database, False otherwise
        """
        return self.use

    def get_read_connection(self):
        """
        Create a singleton read connection if necessary.
        Only called from inside the read() method, this is protected by
        the database.read_mutex of that method.  It is an error to call
        read if the database is not available.

        Arguments:
        <None>
        """
        if sybase_defined and self.use:
            if not database.read_connection:
                logger.info('have credentials')
                # Check that credentials exist
                if not (self.cred_id and self.cred_key):

                    logger.info('No user credentials, so omit '
                                'opening connection to database')
                else:
                    database.read_connection = \
                        Sybase.connect(self.server,
                                       self.cred_id,
                                       self.cred_key,
                                       database=self.read_db,
                                       auto_commit=1,
                                       datetime='python')
                    if not database.read_connection:
                        raise CAOMError('Could not connect to ' +
                                        self.server + ':' +
                                        self.read_db)
        else:
            raise CAOMError('cannot open a read_connection to a database '
                            'because Sybase is not available')

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
        while sybase_defined and retry:
            try:
                logger.debug('acquiring read_mutex...')
                with database.read_mutex:
                    logger.debug(
                        'read_mutex acquired, obtaining connection...')
                    self.get_read_connection()
                    logger.debug(
                        'read_connection obtained, obtaining cursor...')
                    try:
                        cursor = database.read_connection.cursor()
                        logger.debug('cursor obtained, exceuting query...')
                        cursor.execute(query, params)
                        logger.debug('query executed, fetching results...')
                        returnList = cursor.fetchall()
                        logger.debug('results fetched')
                    finally:
                        logger.debug('closing cursor...')
                        cursor.close()
                        logger.debug('cursor closed')
                    retry = False
                    # should be a no-op
                    # database.read_connection.commit()
            except Exception as e:
                # Do not know what kind of error we will get back
                # only the last one will be reported
                if number < len(self.pause_queue):
                    logger.warning('cursor returned error: '
                                   'wait for %.1f seconds and retry',
                                   self.pause_queue[number])
                    t = Event()
                    t.wait(self.pause_queue[number])
                    number += 1
                else:
                    retry = False
                    logger.exception('database read failed')
                    raise
        return returnList

    @classmethod
    def close(cls):
        """
        Close the database conenction

        Arguments:
        cls        the class that called the method (ignored)
        """
        if sybase_defined:
            if database.read_connection:
                database.read_connection.close()
                database.read_connection = None


@contextmanager
def connection(userconfig, use=True):
    """
    Context manager that creates and yields a database object that
    can be used to create read and write connections, then closes the
    connections automatically on completion.

    Arguments:
    server: one of "DEVSYBASE" or "SYBASE"
    database: database to use for credentials
    """
    try:
        yield database(userconfig, use=use)
    finally:
        database.close()
