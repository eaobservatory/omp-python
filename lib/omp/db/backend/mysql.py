# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2015-2017 East Asian Observatory.
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

from __future__ import absolute_import

from contextlib import contextmanager
from threading import Lock
from types import MethodType

import mysql.connector

from omp.error import OMPDBError


class OMPMySQLLock:
    """MySQL lock and cursor management class.
    """

    def __init__(self, server, user, password, read_only=False):
        """Construct object.

        Enabling the read_only option provides some limited protection
        against accidentally writing to the database.  (It prevents
        the transaction method being called with read_write enabled.)
        There doesn't seem to be a way of doing this with DBAPI itself.
        """

        self._read_only = read_only
        self._lock = Lock()
        self._conn = mysql.connector.connect(
            host=server,
            user=user,
            password=password,
            autocommit=False)

    @contextmanager
    def transaction(self, read_write=False):
        """Context manager for database transactions.

        Acquires the lock and provides a cursor.

        If the "read_write" parameter is given, then a commit or rollback
        will be performed depending on whether an error occurs or not.
        Otherwise the cursor will be patched to try to catch some accidental
        attempts to peform queries other than selects.
        """

        if read_write and self._read_only:
            raise OMPDBError(
                'attempt to open read_write transaction on read_only object')

        cursor = None
        success = False

        try:
            self._lock.acquire(True)

            cursor = self._conn.cursor()

            if not read_write:
                # Patch the cursor object so that its execute method checks
                # that the query starts with "SELECT".  This isn't very
                # elegant but there doesn't seem to be an obvious way in
                # which to get a read-only connection or cursor.

                orig_exec = cursor.execute

                def read_only_wrapper(that, query, *args):
                    if not query.upper().startswith('SELECT'):
                        raise OMPDBError(
                            'non-select query in read-only transaction')

                    return orig_exec(query, *args)

                cursor.execute = MethodType(read_only_wrapper, cursor)

            yield cursor

            if read_write:
                self._conn.commit()

        except mysql.connector.Error as e:
            # If we got a database-specific error, re-raise it as our
            # generic error.  Let other exceptions through unchanged.
            # Sybase appears to need us to read the error before
            # closing the cursor?

            if read_write:
                self._conn.rollback()

            raise OMPDBError(str(e))

        except:
            # Also rollback in the case any other error, but then re-raise
            # the exception unchanged.

            if read_write:
                self._conn.rollback()

            raise

        else:
            success = True

        finally:
            if cursor is not None:
                try:
                    cursor.close()

                except Exception as e:
                    # Ignore errors trying to close the cursor if we are
                    # handling an exception, because Sybase can get into
                    # a state where we can't close it!

                    if success:
                        raise OMPDBError('Failed to close cursor: ' + str(e))

            self._lock.release()

    def close(self):
        """Close the database connection."""

        self._conn.close()
