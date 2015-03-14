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

from collections import namedtuple, OrderedDict

from omp.error import OMPError


class OMPState:
    """Class for handling OMP observation states.
    """

    GOOD = 0
    QUESTIONABLE = 1
    BAD = 2
    REJECTED = 3
    JUNK = 4

    _info = OrderedDict()
    _info[GOOD] = 'Good'
    _info[QUESTIONABLE] = 'Questionable'
    _info[BAD] = 'Bad'
    _info[REJECTED] = 'Rejected'
    _info[JUNK] = 'Junk'

    STATE_ALL = tuple(_info.keys())
    STATE_NO_COADD = set((JUNK, BAD))

    @classmethod
    def get_name(cls, state):
        """
        Return the human-readable name of the state.

        Raises OMPError if the state does not exist.
        """

        try:
            return cls._info[state]
        except KeyError:
            raise OMPError('Unknown OMP state code {0}'.format(state))

    @classmethod
    def is_valid(cls, state):
        """
        Check whether a state is valid.

        Returns True if the state exists.
        """

        return state in cls._info
