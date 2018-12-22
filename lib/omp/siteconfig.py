# Copyright (C) 2014 Science and Technology Facilities Council.
# Copyright (C) 2017 East Asian Observatory.
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

try:
    from configparser import SafeConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser

import os

default_site_config_file = '/jac_sw/etc/ompsite.cfg'


def get_omp_siteconfig():
    """Read the OMP site config file.

    Returns a SafeConfigParser object.
    """

    config = SafeConfigParser()

    site_config_file = os.environ.get(
        'OMP_SITE_CONFIG', default_site_config_file)

    config.read(site_config_file)

    return config
