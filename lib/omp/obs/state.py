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

# Information about each state:
#     name: Human-readable name
#     caom_fail: should be marked as fail status in CAOM-2
#     caom_junk: should be marked as junk quality in CAOM-2
OMPStateInfo = namedtuple('OMPStateInfo', ('name', 'caom_fail', 'caom_junk'))


class OMPState:
    """Class for handling OMP observation states.
    """

    GOOD = 0
    QUESTIONABLE = 1
    BAD = 2
    REJECTED = 3
    JUNK = 4

    _info = OrderedDict((
        (GOOD,         OMPStateInfo('Good',        False, False)),
        (QUESTIONABLE, OMPStateInfo('Questionable',True,  False)),
        (BAD,          OMPStateInfo('Bad',         True,  False)),
        (REJECTED,     OMPStateInfo('Rejected',    False, False)),
        (JUNK,         OMPStateInfo('Junk',        True,  True)),
    ))

    STATE_ALL = tuple(_info.keys())
    STATE_NO_COADD = set((JUNK, BAD))

    @classmethod
    def get_name(cls, state):
        """
        Return the human-readable name of the state.

        Raises OMPError if the state does not exist.
        """

        try:
            return cls._info[state].name
        except KeyError:
            raise OMPError('Unknown OMP state code {0}'.format(state))

    @classmethod
    def is_valid(cls, state):
        """
        Check whether a state is valid.

        Returns True if the state exists.
        """

        return state in cls._info

    @classmethod
    def is_caom_fail(cls, state):
        """
        Return whether the state should be marked as a failure in CAOM-2.
        """

        try:
            return cls._info[state].caom_fail
        except KeyError:
            raise OMPError('Unknown OMP state code {0}'.format(state))

    @classmethod
    def is_caom_junk(cls, state):
        """
        Return whether or not the state should be marked junk in CAOM-2.
        """

        try:
            return cls._info[state].caom_junk
        except KeyError:
            raise OMPError('Unknown OMP state code {0}'.format(state))
