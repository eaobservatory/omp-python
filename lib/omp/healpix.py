# Copyright (C) 2013-2015 Science and Technology Facilities Council.
# Copyright (C) 2015-2021 East Asian Observatory.
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

from math import ceil, pi, sqrt
from sys import stderr

import numpy as np
from healpy.pixelfunc import ang2pix, nside2resol
from pymoc import MOC


# Size factor: request this much finer sampling than the size of the
# HEALPix pixels to avoid missing any.
size_factor = 4


def obs_bounds_to_moc(order, obs_bounds, project_info=False):
    nside = 2 ** order
    cells = None
    projects = set()

    for obs_bound in obs_bounds:
        rectangle = obs_bound[0:8]
        if None in rectangle:
            continue

        if project_info:
            projects.add(obs_bound[8])

        try:
            obs_cells = rectangle_to_healpix(nside, True, *rectangle)

            if cells is None:
                cells = obs_cells
            else:
                cells = np.concatenate((cells, obs_cells))
        except ValueError as e:
            print('ValueError: ' + str(e) + ': ' +
                  ' '.join([str(x) for x in obs_bound[8:]]),
                  file=stderr)

    if cells is None:
        moc = None

    else:
        moc = MOC(order, np.unique(cells))

    if project_info:
        return (moc, projects)

    else:
        return moc


def rectangle_to_healpix(nside, nest, *rectangle):
    """
    Generate a unique list of HEALPix pixels for the given rectangle.
    """

    size = nside2resol(nside) * 180 / pi

    ra, dec = rectangle_mesh(size / size_factor, *rectangle)

    # Change from RA, DEC to theta, phi where 0 <= theta <= pi

    return np.unique(ang2pix(nside, pi / 2 - dec * pi / 180,
                             ra * pi / 180, nest=nest))


def rectangle_mesh(size, x_tl, x_bl, x_tr, x_br, y_tl, y_bl, y_tr, y_br):
    """
    Generate a mesh of points covering the given area, with the
    distances between them determined by the size parameter.
    """

    # Check for observations where the RA wraps around.
    if (x_tl < 60 or x_tr < 60 or x_bl < 60 or x_br < 60):
        if x_tl > 300:
            x_tl -= 360

        if x_tr > 300:
            x_tr -= 360

        if x_bl > 300:
            x_bl -= 360

        if x_br > 300:
            x_br -= 360

    dd_b = (x_br - x_bl) ** 2 + (y_br - y_bl) ** 2
    dd_t = (x_tr - x_tl) ** 2 + (y_tr - y_tl) ** 2
    dd_l = (x_tl - x_bl) ** 2 + (y_tl - y_bl) ** 2
    dd_r = (x_tr - x_br) ** 2 + (y_tr - y_br) ** 2

    nx = int(ceil(sqrt(max(dd_b, dd_t)) / size)) + 1
    ny = int(ceil(sqrt(max(dd_l, dd_r)) / size)) + 1

    if nx > 6000 or ny > 6000:
        raise ValueError('Excessively large rectangle')

    fx, fy = np.ogrid[0:1:(nx * 1j), 0:1:(ny * 1j)]

    fx_ = 1 - fx
    fy_ = 1 - fy

    x = fy_ * (x_bl * fx_ + x_br * fx) + \
        fy  * (x_tl * fx_ + x_tr * fx)

    y = fx_ * (y_bl * fy_ + y_tl * fy) + \
        fx  * (y_br * fy_ + y_tr * fy)

    return x.ravel(), y.ravel()
