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

import Sybase
from threading import Lock

from omp.error import OMPDBError


class OMPSybaseLock:
    """Sybase lock and cursor management class.
    """

    def __init__(self, conn):
        """Construct object."""

        self._lock = Lock()
        self._conn = conn

    def __enter__(self):
        """Context manager block entry method.

        Acquires the lock and provides a cursor.
        """

        self._lock.acquire(True)
        self._cursor = self._conn.cursor()
        return self._cursor

    def __exit__(self, type_, value, tb):
        """Context manager  block exit method.

        Closes the cursor and releases the lock.  Since this module
        is intended for read access only, it does not attempt to
        commit a transaction.
        """

        # If we got a database-specific error, re-raise it as our
        # generic error.  Let other exceptions through unchanged.
        # Sybase appears to need us to read the error before
        # closing the cursor?
        new_exc = None
        if type_ is None:
            pass
        elif issubclass(type_, Sybase.Error):
            new_exc = OMPDBError(str(value))

        try:
            self._cursor.close()
            del self._cursor
        except Exception as e:
            # Ignore errors trying to close the cursor if we are
            # handling an exception, because Sybase can get into
            # a state where we can't close it!
            if type_ is None:
                new_exc = OMPDBError('Failed to close cursor: ' + str(e))

        self._lock.release()

        if new_exc is not None:
            raise new_exc

    def close(self):
        """Close the database connection."""

        self._conn.close()
