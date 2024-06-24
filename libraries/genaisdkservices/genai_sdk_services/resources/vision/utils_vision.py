### This code is property of the GGAO ###


# Native imports
from typing import Tuple


def translate(r0: float, c0: float, r1: float, c1: float, origin: tuple, destiny: tuple) -> Tuple[float, float, float, float]:
    """ Translate the coordinates from one image to another

    :param r0: (float) row 0
    :param c0: (float) column 0
    :param r1: (float) row 1
    :param c1: (float) column 1
    :param origin: (tuple) Tuple with the origin image size
    :param destiny: (tuple) Tuple with the destiny image size
    :return: (tuple) Tuple with the new coordinates
    """
    sx0, sy0, sx1, sy1 = c0, r0, c1, r1
    dsx, dsy = destiny
    ssx, ssy = origin

    dy0 = sy0 / ssy * dsy
    dx0 = sx0 / ssx * dsx
    dy1 = sy1 / ssy * dsy
    dx1 = sx1 / ssx * dsx

    # New Origin, the (0,0) is now in the upper left corner
    if r1 < r0:
        dy01 = dsy - dy0
        dy11 = dsy - dy1
    else:
        dy01 = dy0
        dy11 = dy1

    r0 = dy01
    c0 = dx0
    r1 = dy11
    c1 = dx1

    return r0, c0, r1, c1